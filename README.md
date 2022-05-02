# HMS-Mirror

"hms-mirror" is a utility used to bridge the gap between two clusters and migrate `hive` _metadata_.  HMS-Mirror is distributed under the [APLv2](./LICENSE) license.

The application will migrate hive metastore data (metadata) between two clusters.  With [SQL](#sql) and [EXPORT_IMPORT](#export_import) data strategies, we can move data between the two clusters.  While this process functions on smaller datasets, it isn't too efficient for larger datasets.

For the default strategy [SCHEMA_ONLY](#schema-only-and-dump), we can migrate the schemas and sync metastore databases, but the DATA movement is NOT a function of this application.  The application does provide a workbook for each database with SOURCE and TARGET locations.

The output reports are written in [Markdown](https://www.markdownguide.org/).  If you have a client Markdown Renderer like [Marked2](https://marked2app.com/) for the Mac or [Typora](https://typora.io/) which is cross-platform, you'll find a LOT of details in the output reports about what happened.  If you can't install a render, try some web versions [Stackedit.io](https://stackedit.io/app#).  Copy/paste the contents to the report *md* files.

## Table of Contents

<!-- toc -->

- [Quick Start Scenarios](#quick-start-scenarios)
- [WARNING](#warning)
  * [Building METADATA](#building-metadata)
  * [Partition Handling for Data Transfers](#partition-handling-for-data-transfers)
  * [Permissions](#permissions)
- [Features](#features)
  * [VIEWS](#views)
    + [Requirements](#requirements)
  * [ACID Tables](#acid-tables)
    + [The ACID Migration Process](#the-acid-migration-process)
    + [Requirements](#requirements-1)
    + [Replace ACID `-r` or `--replace`](#replace-acid--r-or---replace)
  * [Intermediate/Common Storage Options](#intermediatecommon-storage-options)
  * [Non-Native Hive Tables (Hbase, KAFKA, JDBC, Druid, etc..)](#non-native-hive-tables-hbase-kafka-jdbc-druid-etc)
    + [Requirements](#requirements-2)
  * [AVRO Tables](#avro-tables)
    + [Requirements](#requirements-3)
    + [Warnings](#warnings)
  * [Table Translations](#table-translations)
    + [Legacy Managed Tables](#legacy-managed-tables)
  * [`distcp` Planning Workbook and Scripts](#distcp-planning-workbook-and-scripts)
- [Setup](#setup)
  * [Binary Package](#binary-package)
    + [Don't Build. Download the LATEST binary here!!!](#dont-build-download-the-latest-binary-here)
  * [HMS-Mirror Setup from Binary Distribution](#hms-mirror-setup-from-binary-distribution)
  * [Quick Start](#quick-start)
  * [General Guidance](#general-guidance)
- [Optimizations](#optimizations)
  * [Controlling the YARN Queue that runs the SQL queries from `hms-mirror`](#controlling-the-yarn-queue-that-runs-the-sql-queries-from-hms-mirror)
  * [Make Backups before running `hms-mirror`](#make-backups-before-running-hms-mirror)
  * [Isolate Migration Activities](#isolate-migration-activities)
  * [Speed up CREATE/ALTER Table Statements - with existing data](#speed-up-createalter-table-statements---with-existing-data)
  * [Turn ON HMS partition discovery](#turn-on-hms-partition-discovery)
    + [Source Reference](#source-reference)
- [Pre-Requisites](#pre-requisites)
  * [Backups](#backups)
    + [HDFS Snapshots](#hdfs-snapshots)
    + [Metastore Backups](#metastore-backups)
  * [Shared Authentication](#shared-authentication)
- [Linking Clusters Storage Layers](#linking-clusters-storage-layers)
  * [Goal](#goal)
  * [Scenario #1](#scenario-%231)
    + [HDP 2.6.5 (Hadoop 2.7.x)](#hdp-265-hadoop-27x)
      - [Configuration Changes](#configuration-changes)
    + [CDP 7.1.4 (Hadoop 3.1.x)](#cdp-714-hadoop-31x)
      - [Configuration Changes](#configuration-changes-1)
    + [Running `distcp` from the **RIGHT** Cluster](#running-distcp-from-the-right-cluster)
    + [Sourcing Data from Lower Cluster to Support Upper Cluster External Tables](#sourcing-data-from-lower-cluster-to-support-upper-cluster-external-tables)
      - [Proxy Permissions](#proxy-permissions)
- [Permissions](#permissions-1)
- [Configuration](#configuration)
  * [Secure Passwords in Configuration](#secure-passwords-in-configuration)
    + [Generate the Encrypted Password](#generate-the-encrypted-password)
    + [Running `hms-mirror` with Encrypted Passwords](#running-hms-mirror-with-encrypted-passwords)
- [Tips for Running `hms-miror`](#tips-for-running-hms-miror)
  * [Run in `screen` or `tmux`](#run-in-screen-or-tmux)
  * [Use `dryrun` FIRST](#use-dryrun-first)
  * [Start Small](#start-small)
  * [RETRY (WIP-NOT FULLY IMPLEMENTED YET)](#retry-wip-not-fully-implemented-yet)
- [Running HMS Mirror](#running-hms-mirror)
  * [Assumptions](#assumptions)
    + [Transfer DATA, beyond the METADATA](#transfer-data-beyond-the-metadata)
  * [Options (Help)](#options-help)
  * [Running Against a LEGACY (Non-CDP) Kerberized HiveServer2](#running-against-a-legacy-non-cdp-kerberized-hiveserver2)
    + [Features](#features-1)
      - [BAD_ORC_DEF](#bad_orc_def)
      - [BAD_RC_DEF](#bad_rc_def)
      - [BAD_TEXTFILE_DEF](#bad_textfile_def)
  * [On-Prem to Cloud Migrations](#on-prem-to-cloud-migrations)
    + [SCHEMA_ONLY](#schema_only)
    + [INTERMEDIATE](#intermediate)
  * [Connections](#connections)
    + [Configuring the Libraries](#configuring-the-libraries)
      - [JDBC Connection Strings for HS2](#jdbc-connection-strings-for-hs2)
      - [Non-Kerberos Connections](#non-kerberos-connections)
      - [Kerberized Connections](#kerberized-connections)
      - [ZooKeeper Discovery Connections](#zookeeper-discovery-connections)
      - [TLS/SSL Connections](#tlsssl-connections)
  * [Troubleshooting](#troubleshooting)
    + ["Unrecognized Hadoop major version number: 3.1.1.7.1...0-257"](#unrecognized-hadoop-major-version-number-311710-257)
- [Output](#output)
  * [distcp Workbook (Tech Preview)](#distcp-workbook-tech-preview)
  * [Application Report](#application-report)
  * [Action Report for LEFT and RIGHT Clusters](#action-report-for-left-and-right-clusters)
  * [SQL Execution Output](#sql-execution-output)
  * [Logs](#logs)
- [Strategies](#strategies)
  * [Schema Only and DUMP](#schema-only-and-dump)
  * [LINKED](#linked)
  * [CONVERT_LINKED](#convert_linked)
  * [SQL](#sql)
  * [Export Import](#export-import)
  * [Hybrid](#hybrid)
  * [Common](#common)
  * [Storage Migration](#storage-migration)
- [Troubleshooting / Issues](#troubleshooting--issues)
  * [Failed AVRO Table Creation](#failed-avro-table-creation)
    + [Solution](#solution)
  * [Table processing completed with `ERROR.`](#table-processing-completed-with-error)
    + [Solution](#solution-1)
  * [Connecting to HS2 via Kerberos](#connecting-to-hs2-via-kerberos)
    + [Solution](#solution-2)
  * [Auto Partition Discovery not working](#auto-partition-discovery-not-working)
    + [Solution](#solution-3)
  * [Hive SQL Exception / HDFS Permissions Issues](#hive-sql-exception--hdfs-permissions-issues)
    + [Example and Ambari Hints](#example-and-ambari-hints)
  * [YARN Submission stuck in ACCEPTED phase](#yarn-submission-stuck-in-accepted-phase)
    + [Solution](#solution-4)
  * [Spark DFS Access](#spark-dfs-access)
  * [Permission Issues](#permission-issues)
    + [Solution](#solution-5)
  * [Must use HiveInputFormat to read ACID tables](#must-use-hiveinputformat-to-read-acid-tables)
    + [Solution](#solution-6)
  * [ACL issues across cross while using LOWER clusters storage](#acl-issues-across-cross-while-using-lower-clusters-storage)
    + [Solution](#solution-7)

<!-- tocstop -->

## Quick Start Scenarios

- [On-prem Sidecar Migrations](./use_cases/on-prem_legacy_to_non-legacy.md)

- [Hybrid Migrations](./use_cases/hybrid.md)

## WARNING

### Building METADATA

Rebuilding METADATA can be an expensive scenario.  Especially when you are trying to reconstruct the entire metastore in a short time period, consider this in your planning.  Know the number of partitions and buckets you will be moving and account for this.  Test on smaller datasets (volume and metadata elements).  Progress to testing higher volumes/partition counts to find limits and make adjustments to your strategy.

Using the SQL and EXPORT_IMPORT strategies will move metadata AND data, but rebuilding the metastore elements can be pretty expensive.  So consider migrating the metadata separately from the data (distcp) and use MSCK on the RIGHT cluster to discover the data.  This will be considerably more efficient.

If you will be doing a lot of metadata work on the RIGHT cluster. That cluster also serves a current user base; consider setting up separate HS2 pods for the migration to minimize the impact on the current user community. [Isolate Migration Activities](#isolate-migration-activities)

### Partition Handling for Data Transfers

There are three settings in the configuration to control how and to what extent we'll attempt to migrate *DATA* for tables with partitions.

For non-ACID/transactional tables the setting in:

```yaml
hybrid:
  exportImportPartitionLimit: 100
  sqlPartitionLimit: 500
```

Control both the `HYBRID` strategy for selecting either `EXPORT_IMPORT` or `SQL` and the `SQL` *LIMIT* for how many partitions we'll attempt.  When the `SQL` limit is exceeded, you will need to use `SCHEMA_ONLY` to migrate the schema followed by `distcp` to move the data.

For ACID/transactional tables, the setting in:

```yaml
migrateACID:
  partitionLimit: 500
```

Effectively draws the same limit as above.

Why do we have these limits?  Mass migration of datasets via SQL and EXPORT_IMPORT with many partitions is costly and NOT very efficient.  It's best that when these limits are reached that you separate the METADATA and DATA migration to DDL and distcp.

### Permissions

We use a cross-cluster technique to back metadata in the RIGHT cluster with datasets in the LEFT cluster for data strategies: LINKED, HYBRID, EXPORT_IMPORT, SQL, and SCHEMA_ONLY (with `-ams` AVRO Migrate Schema).

See [Linking Clusters Storage Layers](#linking-clusters-storage-layers) for details on configuring this state.

## Features

`hms-mirror` is designed to migrate schema definitions from one cluster to another or simply provide an extract of the schemas via `-d DUMP`.

Under certain conditions, `hms-mirror` will 'move' data too.  Using the data strategies `-d SQL|EXPORT_IMPORT|HYBRID` well use a combination of SQL temporary tables and [Linking Clusters Storage Layers](#linking-clusters-storage-layers) to facilitate this.

### VIEWS

`hms-mirror` now supports the migration of VIEWs between two environments.  Use the `-v|--views-only` option to execute this path.  VIEW creation requires dependent tables to exist.

Run `hms-mirror` to create all the target tables before running it with the `-v` option.

This flag is an `OR` for processing VIEW's `OR` TABLE's.  They are NOT processed together.

#### Requirements

- The dependent tables must exist in the RIGHT cluster
- When using `-dbp|--db-prefix` option, VIEW definitions are NOT modified and will most likely cause VIEW creation to fail.

### ACID Tables

`hms-mirror` supports the migration of ACID tables using the `-d HYBRID` data strategy in combination with the `-ma|--migrate-acid` or `-mao|--migrate-acid-only` flag.   You can also simply 'replay' the schema definition (without data) using `-d SCHEMA_ONLY -ma|-mao`.  The `-ma|-mao` flag takes an *optional* integer value that sets an 'Artificial Bucket Threshold'.  When no parameter is specified, the default is `2`.

Use this value to set a bucket limit where we'll *remove* the bucket definition during the translation.  This is helpful for legacy ACID tables which *required* a bucket definition but weren't a part of the intended design.  The migration provides an opportunity to correct this artificial design element.

With the default value `2`, we will *remove* CLUSTERING from any ACID table definitions with `2` or fewer buckets defined.  If you wish to keep ALL CLUSTERED definitions, regardless of size, set this value to `0`.

There is now an option to 'downgrade' ACID tables to EXTERNAL/PURGE during migration using the `-da` option.  

#### The ACID Migration Process

The ACID migration builds a 'transfer' table on the LEFT cluster, a 'legacy' managed table (when the LEFT is a legacy cluster), or an 'EXTERNAL/PURGE' table.  Data is copied to this transfer table from the original ACID table via SQL.

Since the clusters are [linked](#linking-clusters-storage-layers), we build a 'shadow' table that is 'EXTERNAL' on the 'RIGHT' cluster that uses the data in the 'LEFT' cluster.  Similar to the LINKED data strategy.  If the data is partitioned, we run `MSCK` on this 'shadow' table in the 'RIGHT' cluster to discover all the partitions.

The final ACID table is created in the 'RIGHT' cluster, and SQL is used to copy data from the 'LEFT' cluster via the 'shadow' table.

#### Requirements

- Data Strategy: `HYBRID`, `SQL`, or `EXPORT_IMPORT`
- Activate Migrate ACID: `-ma|-mao`
- [Link Clusters](#linking-clusters-storage-layers), unless using the `-is|--intermediate-storage` option.
- This is a 'ONE' time transfer.  It is not an incremental update process.
- Adequate Storage on LEFT to make an 'EXTERNAL' copy of the ACID table.
- Permissions:
  - From the RIGHT cluster, the submitting user WILL need access to the LEFT cluster's storage layer (HDFS) to create the shadow table (with location) that points across clusters.
  - doas will have a lot to do with the permissions requirements.
  - The 'hive' service account on the RIGHT cluster will need elevated privileges to the LEFT storage LAYER (HDFS).  For example: If the hive service accounts on each cluster DO NOT share the same identity, like `hive`, then the RIGHT hive identity MUST also have privileged access to the LEFT clusters HDFS layer.
- Partitioned tables must have data that is 'discoverable' via `MSCK`.
  NOTE: The METADATA activity and REDUCER restrictions to the number of BUCKETs can dramatically affect this.- The number of partitions in the source ACID tables must be below the `partitionLimit` (default 500).  This strategy may not be successful when the partition count is above this, and we won't even attempt the conversion. Check YARN for the progress of jobs with a large number of partitions/buckets.  Progress many appear stalled from 'hms-mirror'.
- ACID table migration to Hive 1/2 is NOT supported due to the lack of support for "INSERT OVERWRITE" on transactional tables.  Hive 1/2 to Hive 3 IS support and the target of this implementation.  Hive 3 to Hive 3 is also supported.

#### Replace ACID `-r` or `--replace`

When downgrading ACID tables during migration, the `-r` option will give you the option to 'replace' the original ACID table with the a table that is no longer ACID.  This option is only available along with the `-da` and `SQL` data strategy options.

### Intermediate/Common Storage Options

When bridging the gap between two clusters, you may find they can't share/link storage. In this case, using one of these options will help you with the transfer.

The `-is` or `--intermediate-storage` option is consider a transient location that both cluster can share, see, and have access to.  The strategies for transferring data (EXPORT_IMPORT, SQL, HYBRID) will use this location to facilitate the transfer.  This is a common strategy when migrating from on-prem environments to the cloud.

The `-cs` or `--common-storage` option is similar to `-is` but this option ends up being the final resting place for the data, not just the transfer location.  And with this option, we can streamline the jumps required to migrate data.  Again, this location needs to be accessible to both clusters.


### Non-Native Hive Tables (Hbase, KAFKA, JDBC, Druid, etc..)

Any table definition without a `LOCATION` element is typically a reference to an external system like: HBase, Kafka, Druid, and/or (but not limited to) JDBC.  

#### Requirements

These references require the environment to be:
- Correctly configured to use these resources
- Include the required libraries in the default hive environment.
- The referenced resource must exist already BEFORE the 'hive' DDL will successfully run.

### AVRO Tables

AVRO tables can be designed with a 'reference' to a schema file in `TBLPROPERTIES` with `avro.schema.url`.  The referenced file needs to be 'copied' to the *RIGHT* cluster BEFORE the `CREATE` statement for the AVRO table will succeed.

Add the `-asm|--avro-schema-move` option at the command line to *copy* the file from the LEFT cluster to the RIGHT cluster.

As long as the clusters are [linked](#linking-clusters-storage-layers) and the cluster `hcfsNamespace` values are accurate, the user's credentials running `hms-mirror` will attempt to copy the schema file to the *RIGHT* cluster BEFORE executing the `CREATE` statement.

#### Requirements

- [Link Clusters](#linking-clusters-storage-layers) for Data Strategies: `SCHEMA_ONLY`, `SQL`, `EXPORT_IMPORT`, and `HYBRID`
- Running user must have 'namespace' access to the directories identified in the `TBLPROPERTIES` key `avro.schema.url`.
- The user running `hms-mirror` will need enough storage level permissions to copy the file.
- When hive is running with `doas=false`, `hive` will need access to this file.

#### Warnings

- With the `EXPORT_IMPORT` strategy, the `avro.schema.url` location will NOT be converted. It may lead to an issue reading the table if the location includes a prefix of the cluster's namespace OR the file doesn't exist in the new cluster.

### Table Translations

#### Legacy Managed Tables
`hms-mirror` will convert 'legacy' managed tables in Hive 1 or 2 to EXTERNAL tables in Hive 3.  It relies on the `legacyHive` setting in the cluster configurations to accurately make this conversion.  So make sure you've set this correctly.

### `distcp` Planning Workbook and Scripts

`hms-mirror` will create source files and a shell script that can be used as the basis for the 'distcp' job(s) used to support the databases and tables requested in `-db`.  `hms-mirror` will NOT run these jobs.  It will provide the basic job constructs that match what it did for the schemas.  Use these constructs to build your execution plan and run these separately.

The constructs created are intended as a *one-time* transfer.  If you are using *SNAPSHOTS* or `--update` flags in `distcp` to support incremental updates, you will have to make additional modifications to the scripts/process.  Note: For these scenarios, `hms-mirror` supports options like `-ro|--read-only` and `-sync`.

Each time `hms-mirror` is run, *source* files for each database are created.  These source files need to be copied to the distributed filesystem and reference with an `-f` option in `distcp`.  We also create a *basic* shell script that can be used as a template to run the actual `distcp` jobs.

Depending on the job size and operational expectations, you may want to use *SNAPSHOTS* to ensure an immutable source or use a `diff` strategy for more complex migrations.  Regardless, you'll need to make modifications to these scripts to suit your purposes.

If your process requires the data to exist BEFORE you migrate the schemas, run `hms-mirror` in the `dry-run` mode (default) and use the distcp planning workbook and scripts to transfer the datasets.  Then run `hms-mirror` with the `-e|--execute` option to migrate the schemas.

These workbooks will NOT include elements for ACID/Transactional tables.  Simply copying the dataset for transactional tables will NOT work.  Use the `HYBRID` data strategy migration transactional table schemas and datasets.

## Setup

### Binary Package

#### Don't Build. Download the LATEST binary here!!!
[![Download the LATEST Binary](./images/download.png)](https://github.com/dstreev/hms-mirror/releases)

### HMS-Mirror Setup from Binary Distribution

On the edgenode:
- Remove previous install directory `rm -rf hms-mirror-install`
- Expand the tarball `tar zxvf hms-mirror-dist.tar.gz`.
  > This produces a child `hms-mirror-install` directory.
- Two options for installation:
  - As the root user (or `sudo`), run `hms-mirror-install/setup.sh`. This will install the `hms-mirror` packages in `/usr/local/hms-mirror` and create symlinks for the executables in `/usr/local/bin`.  At this point, `hms-mirror` should be available to all user and in the default path.
  - As the local user, run `hms-mirror-install/setup.sh`.  This will install the `hms-mirror` packages in `$HOME/.hms-mirror` and create symlink in `$HOME/bin`.  Ensure `$HOME/bin` is in the users path and run `hms-mirror`.

*DO NOT RUN `hms-mirror` from the installation directory.*

If you install both options, your environment PATH will determine which one is run.  Make note of this because an upgrade may not be reachable.

### Quick Start

`hms-mirror` requires a configuration file describing the LEFT (source) and RIGHT (target) cluster connections.  There are two ways to create the config:

- `hms-mirror --setup` - Prompts a series of questions about the LEFT and RIGHT clusters to build the default configuration file.
- Use the [default config template](configs/default.template.yaml) as a starting point.  Edit and place a copy here `$HOME/.hms-mirror/cfg/default.yaml`.

If either or both clusters are Kerberized, please review the detailed configuration guidance [here](#running-against-a-legacy-non-cdp-kerberized-hiveserver2) and [here](#kerberized-connections).

`hms-mirror` is designed to RUN from the *RIGHT* cluster.  The *RIGHT* cluster is usually the newer cluster version.  Regardless, under specific scenario's, `hms-mirror` will use an HDFS client to check directories and move small amounts of data (AVRO schema files).  `hms-mirror` will depend on the configuration the node it's running on to locate the 'hcfs filesystem'.  This means that the `/etc/hadoop/conf` directory should contain all the environments settings to successfully connect to `hcfs`.

Under certain conditions, you may need to run `hms-mirror` from the *LEFT* cluster when visibility is directional LEFT->RIGHT, as is usually the case when the LEFT cluster is *on-prem* and the RIGHT cluster is hosted (AWS, Azure, Google).'

### General Guidance

- Run `hms-mirror` from the RIGHT cluster on an Edge Node.
> `hms-mirror` is built (default setting) with CDP libraries and will connect natively using those libraries.  The edge node should have the hdfs client installed and configured for that cluster.  The application will use this connection for some migration strategies.
- If running from the LEFT cluster, note that the `-ro/--read-only` feature examines HDFS on the RIGHT cluster.  The HDFS client on the LEFT cluster may not support this access.
- Connecting to HS2 through KNOX (in both clusters, if possible) reduces the complexities of the connection by removing Kerberos from the picture.
- The libraries will only support a Kerberos connection to a 'single' version of Hadoop at a time.  This is relevant for 'Kerberized' connections to Hive Server 2.  The default libraries will support a kerberized connection to a CDP clusters HS2 and HDFS.  If the LEFT (source) cluster is Kerberized, including HS2, you will need to make some adjustments.
  - The LEFT clusters HS2 needs to support any auth mechanism BUT Kerberos.
  - Use an Ambari Group to setup an independent HS2 instance for this exercise or use KNOX.

## Optimizations

Moving metadata and data between two clusters is a pretty straightforward process but depends entirely on the proper configurations in each cluster.  Listed here are a few tips on some crucial configurations.

NOTE: HMS-Mirror only moves data with the [SQL](#sql) and [EXPORT_IMPORT](#export-import) data strategies.  All other strategies either use the data as-is ([LINKED](#linked) or [COMMON](#common)) or depend on the data being moved by something like `distcp`.

### Controlling the YARN Queue that runs the SQL queries from `hms-mirror`

Use the jdbc url defined in `default.yaml` to set a queue.

`jdbc:hive2://host:10000/.....;...?tez.queue.name=batch`

### Make Backups before running `hms-mirror`

Take snapshots of areas you'll touch:
- The HMS database on the LEFT and RIGHT clusters
- A snapshot of the HDFS directories on BOTH the LEFT and RIGHT clusters will be used/touched.
  > NOTE: If you are testing and "DROPPING" dbs, Snapshots of those data directories could protect you from accidental deletions if you don't manage purge options correctly.  Don't skip this...
  > A snapshot of the db directory on HDFS will prevent `DROP DATABASE x CASCADE` from removing the DB directory (observed in CDP 7.1.4+ as tested, check your version) and all sub-directories even though tables were NOT configured with `purge` options.


### Isolate Migration Activities

The migration of schemas can put a heavy load on HS2 and the HMS server it's using.  That impact can manifest itself as 'pauses' for other clients trying to run queries. Extended schema/discovery operations have a 'blocking' tendency in HS2.

To prevent average user operational impact, I suggest establishing an isolated HMS and HS2 environment for the migration process.
![Isolate Migration Service Endpoints](./images/isolation.png)

### Speed up CREATE/ALTER Table Statements - with existing data

Set `ranger.plugin.hive.urlauth.filesystem.schemes=file` in the Hive Server 2(hive_on_tez) Ranger Plugin Safety Value, via Cloudera Manager.

![Safety Value](./images/hs2_ranger_schemas.png)

Add this to the HS2 instance on the RIGHT cluster when Ranger is used for Auth.
This skips the check done against every directory at the table location (for CREATE or ALTER LOCATION). It is allowing the process of CREATE/ALTER to run much faster.

The default (true) behavior works well for the interactive use case. Still, bulk operations like this can take a long time if this validation needs to happen for every new partition during creation or discovery.

I recommend turning this back after the migration is complete.  This setting exposes permissions issues at the time of CREATE/ALTER.  So by skipping this, future access issues may arise if the permissions aren't aligned, which isn't a Ranger/Hive issue, it's a permissions issue.

### Turn ON HMS partition discovery

In CDP 7.1.4 and below, the housekeeping threads in HMS used to discover partitions are NOT running.  Add `metastore.housekeeping.threads.on=true` to the HMS Safety Value to activate the partition discovery thread.  Once this has been set, the following parameters can be used to modify the default behavior.

```
hive.metastore.partition.management.task.frequency
hive.exec.input.listing.max.threads
hive.load.dynamic.partitions.thread
hive.metastore.fshandler.threads 
```

#### Source Reference

```
    METASTORE_HOUSEKEEPING_LEADER_HOSTNAME("metastore.housekeeping.leader.hostname",
            "hive.metastore.housekeeping.leader.hostname", "",
"If multiple Thrift metastore services are running, the hostname of Thrift metastore " +
        "service to run housekeeping tasks at. By default, this value is empty, which " +
        "means that the current metastore will run the housekeeping tasks. If configuration" +
        "metastore.thrift.bind.host is set on the intended leader metastore, this value should " +
        "match that configuration. Otherwise it should be same as the hostname returned by " +
        "InetAddress#getLocalHost#getHostName(). Given the uncertainty in the later " +
        "it is desirable to configure metastore.thrift.bind.host on the intended leader HMS."),
    METASTORE_HOUSEKEEPING_THREADS_ON("metastore.housekeeping.threads.on",
        "hive.metastore.housekeeping.threads.on", false,
        "Whether to run the tasks under metastore.task.threads.remote on this metastore instance or not.\n" +
            "Set this to true on one instance of the Thrift metastore service as part of turning\n" +
            "on Hive transactions. For a complete list of parameters required for turning on\n" +
            "transactions, see hive.txn.manager."),
```

The default batch size for partition discovery via `msck` is 3000.  Adjustments to this can be made via the `hive.msck.repair.batch.size` property in HS2.

## Pre-Requisites

### Backups

DO NOT SKIP THIS!!!

The `hms-mirror` process DOES 'DROP' tables when asked.  If those tables *manage* data like a *legacy* managed, *ACID*, or *external.table.purge=true* scenario, we do our best NOT to DROP those and ERROR out.  But, protect yourself and make backups of the areas you'll be working in.

#### HDFS Snapshots

Use HDFS Snapshots to make a quick backup of directories you'll be working on.  Do this, especially in the *LEFT* cluster.  We only drop tables, so a snapshot of the database directory is good.  BUT, if you are manually doing any `DROP DATABASE <x> CASCADE` operations, that will delete the snapshotted directory (and the snapshot).  In this case, create the _snapshot_ one level above the database directory.

#### Metastore Backups

Take a DB backup of your metastore and keep it in a safe place before starting.

### Shared Authentication

The clusters must share a common authentication model to support cross-cluster HDFS access when HDFS is the underlying storage layer for the datasets.  This means that a **kerberos** ticket used in the RIGHT cluster must be valid for the LEFT cluster.

For cloud storage, the two clusters must have rights to the target storage bucket.

If you can [`distcp` between the clusters](#linking-clusters-storage-layers), you have the basic connectivity required to start working with `hms-mirror`.

## Linking Clusters Storage Layers

For the `hms-mirror` process to work, it relies on the RIGHT clusters' ability to _SEE_ and _ACCESS_ data in the LEFT clusters HDFS namespace.  This is the same access/configuration required to support DISTCP for an HA environment and accounts for failovers.

We suggest that `distcp` operations be run from the RIGHT cluster, which usually has the greater 'hdfs' version in a migration scenario.

The RIGHT cluster HCFS namespace requires access to the LEFT clusters HCFS namespace.  RIGHT clusters with a greater HDFS version support **LIMITED** functionality for data access in the LEFT cluster.

NOTE: This isn't designed to be a permanent solution and should only be used for testing and migration purposes.

### Goal

What does it take to support HDFS visibility between these two clusters?

Can that integration be used to support the Higher Clusters' use of the Lower Clusters HDFS Layer for distcp AND Hive External Table support?

### Scenario #1

#### HDP 2.6.5 (Hadoop 2.7.x)
Kerberized - sharing same KDC as CDP Base Cluster

##### Configuration Changes

The _namenode_ *kerberos* principal MUST be changed from `nn` to `hdfs` to match the namenode principal of the CDP cluster.

Note: You may need to add/adjust the `auth_to_local` settings to match this change.

If this isn't done, `spark-shell` and `spark-submit` will fail to initialize.  When changing this in Ambari on HDP, you will need to *reset* the HDFS zkfc `ha` zNode in Zookeeper and reinitialize the hdfs `zkfc`.

From a Zookeeper Client: `/usr/hdp/current/zookeeper-client/bin/zkCli.sh -server localhost`
```
rmr /hadoop-ha
```

Initialize zkfc
```
hdfs zkfc -formatZK
```

_core-site.xml_
```
hadoop.rpc.protection=true
dfs.encrypt.data.transfer=true
dfs.encrypt.data.transfer.algorithm=3des
dfs.encrypt.data.transfer.cipher.key.bitlength=256
```

#### CDP 7.1.4 (Hadoop 3.1.x)
Kerberized, TLS Enabled

##### Configuration Changes

Requirements that allow this (upper) cluster to negotiate and communicate with the lower environment.

_Cluster Wide hdfs-site.xml Safety Value_

```
ipc.client.fallback-to-simple-auth-allowed=true
```

_HDFS Service Advanced Config hdfs-site.xml_

```
# For this Clusters Name Service
dfs.internal.nameservices=HOME90

# For the target (lower) environment HA NN Services
dfs.ha.namenodes.HDP50=nn1,nn2
dfs.namenode.rpc-address.HDP50.nn1=k01.streever.local:8020
dfs.namenode.rpc-address.HDP50.nn2=k02.streever.local:8020
dfs.namenode.http-address.HDP50.nn1=k01.streever.local:50070
dfs.namenode.http-address.HDP50.nn2=k02.streever.local:50070
dfs.namenode.https
 address.HDP50.nn1=k01.streever.local:50471
dfs.namenode.https-address.HDP50.nn2=k02.streever.local:50470
dfs.client.failover.proxy.provider.HDP50=org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider

# For Available Name Services
dfs.nameservices=HOME90,HDP50  
```

#### Running `distcp` from the **RIGHT** Cluster

NOTE: Running `distcp` from the **LEFT** cluster isn't supported since the `hcfs client` is not forward compatible.

Copy 'from' Lower Cluster

```
hadoop distcp hdfs://HDP50/user/dstreev/sstats/queues/2020-10.txt /user/dstreev/temp
```

Copy 'to' Lower Cluster

```
hadoop distcp /warehouse/tablespace/external/hive/cvs_hathi_workload.db/queue/2020-10.txt hdfs://HDP50/user/dstreev/temp
```

#### Sourcing Data from Lower Cluster to Support Upper Cluster External Tables

##### Proxy Permissions
The lower cluster must allow the upper clusters _Hive Server 2_ host as a 'hive' proxy.  The setting in the lower clusters _custom_ `core-site.xml` may limit this to that clusters (lower) HS2 hosts.  Open it up to include the upper clusters HS2 host.

_Custom core-site.xml in Lower Cluster_
```
hadoop.proxyuser.hive.hosts=*
```

Credentials from the 'upper' cluster will be projected down to the 'lower' cluster.  The `hive` user in the upper cluster, when running with 'non-impersonation' will require access to the datasets in the lower cluster HDFS.

For table creation in the 'upper' clusters Metastore, a permissions check will be done on the lower environments directory for the submitting user.  So, both the service user AND `hive` will require access to the directory location specified in the lower cluster.

When the two clusters _share_ accounts, and the same accounts are used between environments for users and service accounts, then access should be simple.

When a different set of accounts are used, the 'principal' from the upper clusters service account for 'hive' and the 'user' principal will be used in the lower cluster.  This means additional HDFS policies in the lower cluster may be required to support this cross-environment work.

## Permissions

In both the METADATA and STORAGE phases of `hms-mirror` the RIGHT cluster will reach down into the LEFT clusters storage layer to either _use_ or _copy_ the data.

`hms-mirror` access each cluster via JDBC and use the RIGHT cluster for *storage* layer access.

When the RIGHT cluster is using 'non-impersonation' (hive `doas=false`), the *hive* service account on the **RIGHT** cluster (usually `hive`) needs access to the storage layer on the **LEFT** cluster to use this data to support sidecar testing, where we use the data of the LEFT cluster but *mirror* the metadata.

> Having Ranger on both clusters helps because you can create additional ACLs to provide the access required.

**OR**

>Checked permissions of '<submitting_user>': Found that the '<submitting_user>' user was NOT the owner of the files in these directories. The user running the process needs to be in 'dfs.permissions.superusergroup' for the LEFT clusters 'hdfs' service.  Ambari 2.6 has issues setting this property: https://jira.cloudera.com/browse/EAR-7805

> Follow workaround above or add user to the 'hdfs' group. I had to use '/var/lib/ambari-server/resources/scripts/configs.py' to set it manually for Ambari.

> `sudo ./configs.py --host=k01.streever.local --port=8080 -u admin -p admin -n hdp50 -c hdfs-site -a set -k dfs.permissions.superusergroup -v hdfs_admin`

## Configuration

The configuration is done via a 'yaml' file, details below.

There are two ways to get started:
- The first time you run `hms-mirror` and it can't find a configuration, it will walk you through building one and save it to `$HOME/.hms-mirror/cfg/default.yaml`.  Here's what you'll need to complete the setup:
  - URI's for each clusters HiveServer2
  - STANDALONE jar files for EACH Hive version.
  - Username and Password for non-kerberized connections.
    - Note: `hms-mirror` only support one kerberos connection.  For the other, use another AUTH method.
  - The hcfs (Hadoop Compatible FileSystem) protocol and prefix used for the hive table locations in EACH cluster.
- Use the [template yaml](./configs/default.template.yaml) for reference and create a `default.yaml` in the running users `$HOME/.hms-mirror/cfg` directory.

You'll need JDBC driver jar files that are **specific* to the clusters you'll integrate.  If the **LEFT** cluster isn't the same version as the **RIGHT** cluster, don't use the same JDBC jar file, especially when integrating Hive 1 and Hive 3 services.  The Hive 3 driver is NOT backwardly compatible with Hive 1.

See the [running](#running-hms-mirror) section for examples on running `hms-mirror` for various environment types and connections.

### Secure Passwords in Configuration

There are two passwords stored in the configuration file mentioned above.  One for each 'JDBC connection, if those rely on a password for connect.  By default, the passwords are in clear text in the configuration file.  This usually isn't an issue since the file can be protected at the UNIX level from peering eyes.  But if you need to protect those passwords, `hms-mirror` supports storing an encrypted version of the password in the configuration.

The `password` element for each JDBC connection can be replaced with an **encrypted** version of the password and read by `hms-mirror` during execution, so the clear text version of the password isn't persisted anywhere.

When you're using this feature, you need to have a `password-key`.  This is a key used to encrypt and decrypt the password in the configuration.  The same `password-key` must be used for each password in the configuration file.

#### Generate the Encrypted Password

Use the `-pkey` and `-p` options of `hms-mirror`

`hms-mirror -pkey cloudera -p have-a-nice-day`

Will generate:
```
...
Encrypted password: HD1eNF8NMFahA2smLM9c4g==
```

Copy this encrypted password and place it in your configuration file for the JDBC connection.  Repeat for the other passwords, if it's different, and paste it in the configuration as well.

#### Running `hms-mirror` with Encrypted Passwords

Using the **same** `-pkey` you used to generate the encrypted password, we'll run `hms-mirror`

`hms-mirror -db <db> -pkey cloudera ...`

When the `-pkey` option is specified **WITHOUT** the `-p` option (used previously), `hms-mirror` will understand to **decrypt** the configuration passwords before connecting to jdbc.  If you receive jdbc connection exceptions, recheck the `-pkey` and encrypted password from before.

## Tips for Running `hms-miror`

### Run in `screen` or `tmux`

This process can be a long-running process.  It depends on how much you've asked it to do.  Having the application terminated because the `ssh` session to the edgenode timed out and your computer went to sleep will be very disruptive.

Using either of these session state tools (or another of your choice) while running it on an edgenode will allow you to sign-off without disrupting the process AND reattach to see the interactive progress at a later point.

### Use `dryrun` FIRST

Before you run a process that will make changes, try running `hms-mirror` with the `dry-run` option first.  The report generated at the end of the job will provide insight into what issues (if any) you'll run across.

### Start Small

Use `-db`(database) AND `-tf`(table filter) options to limit the scope of what you're processing.  Start with a test database that contains various table types you'd like to migrate.

Review the output report for details/problems you encountered in processing.

Since the process expects the user to have adequate permissions in both clusters, you may find you have some prework to do before continuing.

### RETRY (WIP-NOT FULLY IMPLEMENTED YET)

When you run `hms-mirror` we write a `retry` file out to the `$HOME/.hms-mirror/retry` directory with the same filename prefix as the config used to run the job.  If you didn't specify a config, it uses the `$HOME/.hms-mirror/cfg/default.yaml` file as a default.  This results in a `$HOME/.hms-mirror/retry/default.retry` file.

If the job fails partway through you can rerun the process with the `--retry` option and it will load the _retry_ file and retry any operation that wasn't previously successful.  'Retry' is a tech-preview function and not thoroughly tested.


## Running HMS Mirror

After running the `setup.sh` script, `hms-mirror` will be available in the `$PATH` in a default configuration.

### Assumptions

1. This process will only 'migrate' EXTERNAL and MANAGED (non-ACID/Transactional) table METADATA (not data, except with [SQL](#sql) and [EXPORT_IMPORT](#export-import) ).
2. MANAGED tables replicated to the **RIGHT** cluster will be converted to "EXTERNAL" tables for the 'metadata' stage.  They will be tagged as 'legacy managed' in the **RIGHT** cluster and will NOT be assigned the `external.table.purge=true` flag, yet.  Once the table's data has been migrated, the table will be adjusted to be purgeable via `external.table.purge=true` to match the classic `MANAGED` table behavior.
1. The **RIGHT** cluster has 'line of sight' to the **LEFT** cluster.
2. The **RIGHT** cluster has been configured to access the **LEFT** cluster storage. See [link clusters](#linking-clusters-storage-layers).  This is the same configuration required to support `distcp` from the **RIGHT** cluster to the **LEFT** cluster.
3. The movement of metadata/data is from the **LEFT** cluster to the **RIGHT** cluster.
4. With Kerberos, each cluster must share the same trust mechanism.
  - The **RIGHT** cluster must be Kerberized IF the **LEFT** cluster is.
  - The **LEFT** cluster does NOT need to be kerberized if the **RIGHT** cluster is kerberized.
7. The **LEFT* cluster does NOT have access to the **RIGHT** cluster.
8. The credentials use by 'hive' (doas=false) in the **RIGHT** cluster must have access to the required storage (hdfs) locations on the lower cluster.
  - If the **RIGHT** cluster is running impersonation (doas=true), that user must have access to the required storage (hdfs) locations on the lower cluster.

#### Transfer DATA, beyond the METADATA

HMS-Mirror does NOT migrate data between clusters unless you're using the [SQL](#sql) and [EXPORT_IMPORT](#export-import) data strategies.  In some cases where data is co-located, you don't need to move it.  IE: Cloud to Cloud.  As long as the new cluster environment has access to the original location.  This is the intended target for strategies [COMMON](#common) and to some extend [LINKED](#linked).

When you do need to move data, `hms-mirror` create a workbook of 'source' and 'target' locations in an output file called `distcp_workbook.md`.  [Sample](sample_reports/schema_only/distcp_workbook.md).  Use this to help build a transfer job in `distcp` using the `-f` option to specify multiple sources.  This construct is still a work in progress, so feedback is welcome [Email - David Streever](mailto:dstreever@cloudera.com).

### Options (Help)

```
usage: hms-mirror <options>

Hive Metastore Migration Utility
 -accept,--accept                                                          Accept ALL confirmations
                                                                           and silence prompts
 -ap,--acid-partition-count <limit>                                        Set the limit of
                                                                           partitions that the ACID
                                                                           strategy will work with.
                                                                           '-1' means no-limit.
 -asm,--avro-schema-migration                                              Migrate AVRO Schema Files
                                                                           referenced in
                                                                           TBLPROPERTIES by
                                                                           'avro.schema.url'.
                                                                           Without migration it is
                                                                           expected that the file
                                                                           will exist on the other
                                                                           cluster and match the
                                                                           'url' defined in the
                                                                           schema DDL.
                                                                           If it's not present,
                                                                           schema creation will
                                                                           FAIL.
                                                                           Specifying this option
                                                                           REQUIRES the LEFT and
                                                                           RIGHT cluster to be
                                                                           LINKED.
                                                                           See docs:
                                                                           https://github.com/dstree
                                                                           v/hms-mirror#linking-clus
                                                                           ters-storage-layers
 -cfg,--config <filename>                                                  Config with details for
                                                                           the HMS-Mirror.  Default:
                                                                           $HOME/.hms-mirror/cfg/def
                                                                           ault.yaml
 -cs,--common-storage <storage-path>                                       Common Storage used with
                                                                           Data Strategy HYBRID,
                                                                           SQL, EXPORT_IMPORT.  This
                                                                           will change the way these
                                                                           methods are implemented
                                                                           by using the specified
                                                                           storage location as an
                                                                           'common' storage point
                                                                           between two clusters.  In
                                                                           this case, the cluster do
                                                                           NOT need to be 'linked'.
                                                                           Each cluster DOES need to
                                                                           have access to the
                                                                           location and
                                                                           authorization to interact
                                                                           with the location.  This
                                                                           may mean additional
                                                                           configuration
                                                                           requirements for 'hdfs'
                                                                           to ensure this seamless
                                                                           access.
 -d,--data-strategy <strategy>                                             Specify how the data will
                                                                           follow the schema. [DUMP,
                                                                           SCHEMA_ONLY, LINKED, SQL,
                                                                           EXPORT_IMPORT, HYBRID,
                                                                           CONVERT_LINKED,
                                                                           STORAGE_MIGRATION,
                                                                           COMMON]
 -da,--downgrade-acid                                                      Downgrade ACID tables to
                                                                           EXTERNAL tables with
                                                                           purge.
 -db,--database <databases>                                                Comma separated list of
                                                                           Databases (upto 100).
 -dbo,--database-only                                                      Migrate the Database
                                                                           definitions as they exist
                                                                           from LEFT to RIGHT
 -dbp,--db-prefix <prefix>                                                 Optional: A prefix to add
                                                                           to the RIGHT cluster DB
                                                                           Name. Usually used for
                                                                           testing.
 -ds,--dump-source <source>                                                Specify which 'cluster'
                                                                           is the source for the
                                                                           DUMP strategy
                                                                           (LEFT|RIGHT).
 -e,--execute                                                              Execute actions request,
                                                                           without this flag the
                                                                           process is a dry-run.
 -ep,--export-partition-count <limit>                                      Set the limit of
                                                                           partitions that the
                                                                           EXPORT_IMPORT strategy
                                                                           will work with.
 -ewd,--external-warehouse-directory <external-warehouse-path>             The external warehouse
                                                                           directory path.  Should
                                                                           not include the namespace
                                                                           OR the database
                                                                           directory. This will be
                                                                           used to set the LOCATION
                                                                           database option.
 -f,--flip                                                                 Flip the definitions for
                                                                           LEFT and RIGHT.  Allows
                                                                           the same config to be
                                                                           used in reverse.
 -h,--help                                                                 Help
 -is,--intermediate-storage <storage-path>                                 Intermediate Storage used
                                                                           with Data Strategy
                                                                           HYBRID, SQL,
                                                                           EXPORT_IMPORT.  This will
                                                                           change the way these
                                                                           methods are implemented
                                                                           by using the specified
                                                                           storage location as an
                                                                           intermediate transfer
                                                                           point between two
                                                                           clusters.  In this case,
                                                                           the cluster do NOT need
                                                                           to be 'linked'.  Each
                                                                           cluster DOES need to have
                                                                           access to the location
                                                                           and authorization to
                                                                           interact with the
                                                                           location.  This may mean
                                                                           additional configuration
                                                                           requirements for 'hdfs'
                                                                           to ensure this seamless
                                                                           access.
 -ma,--migrate-acid <arg>                                                  Migrate ACID tables (if
                                                                           strategy allows).
                                                                           Optional:
                                                                           ArtificialBucketThreshold
                                                                           count that will remove
                                                                           the bucket definition if
                                                                           it's below this.  Use
                                                                           this as a way to remove
                                                                           artificial bucket
                                                                           definitions that were
                                                                           added 'artificially' in
                                                                           legacy Hive. (default: 2)
 -mao,--migrate-acid-only <arg>                                            Migrate ACID tables ONLY
                                                                           (if strategy allows).
                                                                           Optional:
                                                                           ArtificialBucketThreshold
                                                                           count that will remove
                                                                           the bucket definition if
                                                                           it's below this.  Use
                                                                           this as a way to remove
                                                                           artificial bucket
                                                                           definitions that were
                                                                           added 'artificially' in
                                                                           legacy Hive. (default: 2)
 -mnn,--migrate-non-native <arg>                                           Migrate Non-Native tables
                                                                           (if strategy allows).
                                                                           These include table
                                                                           definitions that rely on
                                                                           external connection to
                                                                           systems like: HBase,
                                                                           Kafka, JDBC
 -mnno,--migrate-non-native-only                                           Migrate Non-Native tables
                                                                           (if strategy allows).
                                                                           These include table
                                                                           definitions that rely on
                                                                           external connection to
                                                                           systems like: HBase,
                                                                           Kafka, JDBC
 -o,--output-dir <outputdir>                                               Output Directory
                                                                           (default:
                                                                           $HOME/.hms-mirror/reports
                                                                           /<yyyy-MM-dd_HH-mm-ss>
 -p,--password <password>                                                  Used this in conjunction
                                                                           with '-pkey' to generate
                                                                           the encrypted password
                                                                           that you'll add to the
                                                                           configs for the JDBC
                                                                           connections.
 -pkey,--password-key <password-key>                                       The key used to encrypt /
                                                                           decrypt the cluster jdbc
                                                                           passwords.  If not
                                                                           present, the passwords
                                                                           will be processed as is
                                                                           (clear text) from the
                                                                           config file.
 -q,--quiet                                                                Reduce screen reporting
                                                                           output.  Good for
                                                                           background processes with
                                                                           output redirects to a
                                                                           file
 -r,--replace                                                              When downgrading an ACID
                                                                           table as its transferred
                                                                           to the 'RIGHT' cluster,
                                                                           this option will replace
                                                                           the current ACID table on
                                                                           the LEFT cluster with a
                                                                           'downgraded' table
                                                                           (EXTERNAL). The option
                                                                           only works with options
                                                                           '-da' and '-cs'.
 -rdl,--reset-to-default-location                                          Strip 'LOCATION' from all
                                                                           target cluster
                                                                           definitions.  This will
                                                                           allow the system defaults
                                                                           to take over and define
                                                                           the location of the new
                                                                           datasets.
 -ro,--read-only                                                           For SCHEMA_ONLY, COMMON,
                                                                           and LINKED data
                                                                           strategies set RIGHT
                                                                           table to NOT purge on
                                                                           DROP
 -rr,--reset-right                                                         Use this for testing to
                                                                           remove the database on
                                                                           the RIGHT using CASCADE.
 -s,--sync                                                                 For SCHEMA_ONLY, COMMON,
                                                                           and LINKED data
                                                                           strategies.  Drop and
                                                                           Recreate Schema's when
                                                                           different.  Best to use
                                                                           with RO to ensure
                                                                           table/partition drops
                                                                           don't delete data. When
                                                                           used WITHOUT `-tf` it
                                                                           will compare all the
                                                                           tables in a database and
                                                                           sync (bi-directional).
                                                                           Meaning it will DROP
                                                                           tables on the RIGHT that
                                                                           aren't in the LEFT and
                                                                           ADD tables to the RIGHT
                                                                           that are missing.  When
                                                                           used with `-ro`, table
                                                                           schemas can be updated by
                                                                           dropping and recreating.
                                                                           When used with `-tf`,
                                                                           only the tables that
                                                                           match the filter (on both
                                                                           sides) will be
                                                                           considered.
 -sf,--skip-features                                                       Skip Features evaluation.
 -smn,--storage-migration-namespace <Storage Migration Target Namespace>   Optional: Used with the
                                                                           'data strategy
                                                                           STORAGE_MIGRATION to
                                                                           specify the target
                                                                           namespace.
 -sp,--sql-partition-count <limit>                                         Set the limit of
                                                                           partitions that the SQL
                                                                           strategy will work with.
                                                                           '-1' means no-limit.
 -sql,--sql-output                                                         <deprecated>.  This
                                                                           option is no longer
                                                                           required to get SQL out
                                                                           in a report.  That is the
                                                                           default behavior.
 -su,--setup                                                               Setup a default
                                                                           configuration file
                                                                           through a series of
                                                                           questions
 -tf,--table-filter <regex>                                                Filter tables (inclusive)
                                                                           with name matching RegEx.
                                                                           Comparison done with
                                                                           'show tables' results.
                                                                           Check case, that's
                                                                           important.  Hive tables
                                                                           are generally stored in
                                                                           LOWERCASE.
 -v,--views-only                                                           Process VIEWs ONLY
 -wd,--warehouse-directory <warehouse-path>                                The warehouse directory
                                                                           path.  Should not include
                                                                           the namespace OR the
                                                                           database directory. This
                                                                           will be used to set the
                                                                           MANAGEDLOCATION database
                                                                           option.
```

### Running Against a LEGACY (Non-CDP) Kerberized HiveServer2

`hms-mirror` is pre-built with CDP libraries and they WILL NOT be compatible with LEGACY kerberos environments. A Kerberos connection can only be made to ONE cluster when the clusters are NOT running the same 'major' version of Hadoop.

To attach to a LEGACY HS2, run `hms-mirror` with the `--hadoop-classpath` command-line option.  This will strip the CDP libraries from `hms-mirror` and use the hosts Hadoop libraries by calling `hadoop classpath` to locate the binaries needed to do this.

#### Features

Features are a way to inject special considerations into the replay of a schema between clusters.  Each schema is automatically check is a particular 'feature' applies.  

If you find that this features check is causing issues, add the flag `-sf` to the application parameters and the feature checks will be skipped.

##### BAD_ORC_DEF

`BAD_ORC_DEF` is a feature that corrects poorly executed schema definitions in legacy Hive 1/2 that don't translate into a functioning table in Hive 3.  In this case, the legacy definition was defined with:
```
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '\t'
  LINES TERMINATED BY '\n'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
```

when it should have been created with:

```
STORED AS ORC
```

The result, when not modified and replayed in Hive 3 is a table that isn't functional.  The `BAD_ORC_DEF` feature will replace:

```
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '\t'
  LINES TERMINATED BY '\n'
```

with:

```
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
```

##### BAD_RC_DEF

`BAD_RC_DEF` is a feature that corrects poorly executed schema definitions in legacy Hive 1/2 that doesn't translate into a functioning table in Hive 3.  In this case, the legacy definition was defined with:
```
ROW FORMAT DELIMITED,
    FIELDS TERMINATED BY '|'
 STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.RCFileInputFormat'
 OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.RCFileOutputFormat'
```

when it should have been created with:

```
STORED AS RCFILE
```

The result, when not modified and replayed in Hive 3 is a table that isn't functional.  The `BAD_RC_DEF` feature will replace:

```
ROW FORMAT DELIMITED,                              
    FIELDS TERMINATED BY '|'                       
 STORED AS INPUTFORMAT                             
```

with:

```
STORED AS RCFILE
```

##### BAD_TEXTFILE_DEF

Older Textfile schemas somehow are corrupted through subsequent ALTER statements that get the table into a state where you can NOT re-run the contents of `SHOW CREATE TABLE`.  In this case, the issue is that there is a declaration for `WITH SERDEPROPERTIES` along with a `ROW FORMAT DELIMITED` clause.  These two can NOT exist together.  Here is an example of this:

```
ROW FORMAT DELIMITED
     FIELDS TERMINATED BY '|'
     LINES TERMINATED BY '\n'
WITH SERDEPROPERTIES (
     'escape.delim'='\\')
STORED AS INPUTFORMAT
     'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
     'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
```

In this case, we need to convert the `ROW FORMAT DELIMITED * TERMINATED BY *` values into the `SERDEPROPERTIES` and replace it with 

```
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
```

### On-Prem to Cloud Migrations

On-Prem to Cloud Migrations should run `hms-mirror` from the LEFT cluster since visibility in this scenario is usually restricted to LEFT->RIGHT.

If the cluster is an older version of Hadoop (HDP 2, CDH 5), your connection to the LEFT HS2 should NOT be kerberized.  Use LDAP or NO_AUTH.

The clusters LEFT hcfsNamespace (clusters:LEFT:hcfsNamespace) should be the LEFT clusters HDFS service endpoint.  The RIGHT hcfsNamespace (clusters:RIGHT:hcfsNamespace) should be the *target* root cloud storage location.  The LEFT clusters configuration (/etc/hadoop/conf) should have all the necessary credentials to access this location.  Ensure that the cloud storage connectors are available in the LEFT environment.

There are different strategies available for migrations between on-prem and cloud environments.

#### SCHEMA_ONLY

This is a schema-only transfer, where the `hcfsNamespace` in the metadata definitions is 'replaced' with the `hcfsNamespace` value defined on the RIGHT.  NOTE: The 'relative' directory location is maintained in the migration.

No data will be migrated in this case.

There will be a [`distcp` Planning Workbook](#distcp-planning-workbook) generated with a plan that can be used to build the data migration process with `distcp`.

#### INTERMEDIATE

### Connections

`hms-mirror` connects to 3 endpoints.  The hive jdbc endpoints for each cluster (2) and the `hdfs` environment configured on the running host.  This means you'll need:
- JDBC drivers to match the JDBC endpoints
- For **non** CDP 7.x environments and Kerberos connections, an edge node with the current Hadoop libraries.

See the [config](#configuration) section to setup the config file for `hms-mirror`.

#### Configuring the Libraries

##### JDBC Connection Strings for HS2

See the [Apache docs](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=30758725#HiveServer2Clients-JDBC) regarding these details if you are using the environment 'Standalone' JDBC drivers.  Other drivers may have different connect string requirements.

The drivers for the various environments are located:

- HDP - `/usr/hdp/current/hive-server2/jdbc/hive-jdbc-<version>-standalone.jar` (NOTE: Use the hive-1 standalone jar file for HDP 2.6.5, not the hive-2 jar)
- CDH/CDP - `/opt/cloudera/parcels/CDH/jars/hive-jdbc-<version>-standalone.jar`

##### Non-Kerberos Connections

The most effortless connections are 'non-kerberos' JDBC connections either to HS2 with AUTH models that aren't **Kerberos** or through a **Knox** proxy.  Under these conditions, only the __standalone__ JDBC drivers are required.  Each of the cluster configurations contains an element `jarFile` to identify those standalone libraries.

```yaml
    hiveServer2:
      uri: "<jdbc-url>"
      connectionProperties:
        user: "*****"
        password: "*****"
      jarFile: "<environment-specific-jdbc-standalone-driver>"
```

When dealing with clusters supporting different Hive (Hive 1 vs. Hive 3) versions, the JDBC drivers aren't forward OR backward compatible between these versions.  Hence, each JDBC jar file is loaded in a sandbox that allows us to use the same driver class, but isolates it between the two JDBC jars.

Place the two jdbc jar files in any directory **EXCEPT** `$HOME/.hms-mirror/aux_libs` and reference the full path in the `jarFile` property for that `hiveServer2` configuration.

_SAMPLE Commandline_

`hms-mirror -db tpcds_bin_partitioned_orc_10`

##### Kerberized Connections

`hms-mirror` relies on the Hadoop libraries to connect via 'kerberos'.  Suppose the clusters are running different versions of Hadoop/Hive. In that case, we can only support connecting to one of the clusters via Kerberos.  `hms-mirror` is built with the dependencies for Hadoop 3.1 (CDP 7.1.x).  Kerberos connections are NOT supported in the 'sandbox' configuration we discussed above.

There are three scenarios for kerberized connections.

| Scenario | LEFT Kerberized/Version | RIGHT Kerberized/Version | Notes | Sample Commandline |
|:---|:---:|:---:|:---|:---|
| 1 | No <br/> HDP2 | Yes <br/> HDP 3 or CDP 7 | <ol><li>The hadoop libs are built into `hms-mirror` for this scenario.</li><li>'hms-mirror' needs to be run from a node on the HDP3/CDP cluster.</li><li>place the RIGHT cluster jdbc jar file in `$HOME/.hms-mirror/aux_libs` (yes this contradicts some earlier directions)</li><li>comment out the `jarFile` property for the RIGHT cluster hiveServer2 setting.</li></ol> | `hms-mirror -db tpcds_bin_partitioned_orc_10` |
| 2 | YES <br/> HDP 3 or CDP 7 | YES <br/>HDP 3 or CDP 7 | <ol><li>The hadoop libs are built into `hms-mirror` for this scenario.</li><li>'hms-mirror' needs to be run from a node on the HDP3/CDP cluster.</li><li>place the RIGHT cluster jdbc jar file in $HOME/.hms-mirror/aux_libs (yes this contradicts some earlier directions)</li><li>comment out the `jarFile` property for the LEFT AND RIGHT cluster hiveServer2 settings.</li></ol> | `hms-mirror -db tpcds_bin_partitioned_orc_10` |
| 3 | YES<br/>HDP2 or Hive 1 | NO <br/> HDP 3 or CDP 7 | Not Supported when `hms-mirror` run from the RIGHT cluster. | `hms-mirror -db tpcds_bin_partitioned_orc_10` |
| 4 | YES<br/>HDP2 or Hive 1 | YES <br/> HDP2 or Hive 1 | <ol><li>The Kerberos credentials must be TRUSTED to both clusters</li><li>Add `--hadoop-classpath` as a commandline option to `hms-mirror`.  This replaces the prebuilt Hadoop 3 libraries with the current environments Hadoop Libs.</li><li>Add the jdbc standalone jar file to `$HOME/.hms-mirror/aux_libs`</li><li>Comment out/remove the `jarFile` references for BOTH clusters in the configuration file.</li></ol> | `hms-mirror -db tpcds_bin_partitioned_orc_10 --hadoop-classpath` |

For Kerberos JDBC connections, ensure you are using an appropriate Kerberized Hive URL.

`jdbc:hive2://s03.streever.local:10000/;principal=hive/_HOST@STREEVER.LOCAL`

##### ZooKeeper Discovery Connections

You may run into issues connecting to an older cluster using ZK Discovery.  This mode brings in a LOT of the Hadoop ecosystem classes and may conflict across environments.  We recommend using ZooKeeper discovery on only the RIGHT cluster.  Adjust the LEFT cluster to access HS2 directly.

##### TLS/SSL Connections

If your HS2 connection requires TLS, you will need to include that detail in the jdbc 'uri' you provide.  In addition, if the SSL certificate is 'self-signed' you will need to include details about the certificate to the java environment.  You have 2 options:

- Set the JAVA_OPTS environment with the details about the certificate.
  - `export JAVA_OPTS=-Djavax.net.ssl.trustStore=/home/dstreev/certs/gateway-client-trust.jks -Djavax.net.ssl.trustStorePassword=changeit`
- Add `-D` options to the `hms-mirror` commandline to inject those details.
  - `hms-mirror -db test_db -Djavax.net.ssl.trustStore=/home/dstreev/certs/gateway-client-trust.jks -Djavax.net.ssl.trustStorePassword=changeit`



### Troubleshooting

If each JDBC endpoint is Kerberized and the connection to the LEFT or RIGHT is successful, both NOT both, and the program seems to hang with no exception...  it's most likely that the Kerberos ticket isn't TRUSTED across the two environments.  You will only be able to support a Kerberos connection to the cluster where the ticket is trusted.  The other cluster connection will need to be anything BUT Kerberos.

Add `--show-cp` to the `hms-mirror` command line to see the classpath used to run.

The argument `--hadoop-classpath` allows us to replace the embedded Hadoop Libs (v3.1) with the libs of the current platform via a call to `hadoop classpath`.  This is necessary to connect to kerberized Hadoop v2/Hive v1 environments.

Check the location and references to the JDBC jar files.  General rules for Kerberos Connections:
- The JDBC jar file should be in the `$HOME/.hms-mirror/aux_libs`.  For Kerberos connections, we've seen issues attempting to load this jar in a sandbox, so this makes it available to the global classpath/loader.
- Get a Kerberos ticket for the running user before launching `hms-mirror`.

#### "Unrecognized Hadoop major version number: 3.1.1.7.1...0-257"

This happens when you're trying to connect to an HS2 instance.

## Output

### distcp Workbook (Tech Preview)

We're working through some strategies to help build `distcp` jobs that can be run to migrate data from one environment to the other, based on what you've asked `hms-mirror` to do.  A sample report may look like the table below.

Using [`distcp`](https://hadoop.apache.org/docs/r3.1.4/hadoop-distcp/DistCp.html) with the details below, create a `distcp` job for each 'database/target' value.  Take the contents of the 'sources' and paste them into a text file.  Copy that text file to 'hdfs' and reference it with the `-f` option in `distcp`.

`hadoop distcp -f /user/dstreev/tpcds_distcp_sources.txt hdfs://HOME90/apps/hive/warehouse/tpcds_bin_partitioned_orc_10.db`

This job will use the source list, copy the contents between the clusters appending the 'last' directory in the 'source' line to the 'target.'

Note: Depending on the volume of the underlying datasets, you may need to adjust the `distcp` job to increase its memory footprint.  See the [`distcp` docs](https://hadoop.apache.org/docs/r3.1.4/hadoop-distcp/DistCp.html) details if you have issues with jobs failures.

Also, note that we are NOT considering sourcing the jobs from a SNAPSHOT.  We do recommend that for datasets that are changing while the 'distcp' job runs.

| Database | Target | Sources |
|:---|:---|:---|
| tpcds_bin_partitioned_orc_10 | | |
| | hdfs://HOME90/apps/hive/warehouse/tpcds_bin_partitioned_orc_10.db | hdfs://HDP50/apps/hive/warehouse/tpcds_bin_partitioned_orc_10.db/call_center<br>hdfs://HDP50/apps/hive/warehouse/tpcds_bin_partitioned_orc_10.db/catalog_page<br>hdfs://HDP50/apps/hive/warehouse/tpcds_bin_partitioned_orc_10.db/catalog_returns<br>hdfs://HDP50/apps/hive/warehouse/tpcds_bin_partitioned_orc_10.db/catalog_sales<br>hdfs://HDP50/apps/hive/warehouse/tpcds_bin_partitioned_orc_10.db/customer<br>hdfs://HDP50/apps/hive/warehouse/tpcds_bin_partitioned_orc_10.db/customer_address<br>hdfs://HDP50/apps/hive/warehouse/tpcds_bin_partitioned_orc_10.db/customer_demographics<br>hdfs://HDP50/apps/hive/warehouse/tpcds_bin_partitioned_orc_10.db/date_dim<br>hdfs://HDP50/apps/hive/warehouse/tpcds_bin_partitioned_orc_10.db/household_demographics<br>hdfs://HDP50/apps/hive/warehouse/tpcds_bin_partitioned_orc_10.db/income_band<br>hdfs://HDP50/apps/hive/warehouse/tpcds_bin_partitioned_orc_10.db/inventory<br>hdfs://HDP50/apps/hive/warehouse/tpcds_bin_partitioned_orc_10.db/item<br>hdfs://HDP50/apps/hive/warehouse/tpcds_bin_partitioned_orc_10.db/promotion<br>hdfs://HDP50/apps/hive/warehouse/tpcds_bin_partitioned_orc_10.db/reason<br>hdfs://HDP50/apps/hive/warehouse/tpcds_bin_partitioned_orc_10.db/ship_mode<br>hdfs://HDP50/apps/hive/warehouse/tpcds_bin_partitioned_orc_10.db/store<br>hdfs://HDP50/apps/hive/warehouse/tpcds_bin_partitioned_orc_10.db/store_returns<br>hdfs://HDP50/apps/hive/warehouse/tpcds_bin_partitioned_orc_10.db/store_sales<br>hdfs://HDP50/apps/hive/warehouse/tpcds_bin_partitioned_orc_10.db/time_dim<br>hdfs://HDP50/apps/hive/warehouse/tpcds_bin_partitioned_orc_10.db/warehouse<br>hdfs://HDP50/apps/hive/warehouse/tpcds_bin_partitioned_orc_10.db/web_page<br>hdfs://HDP50/apps/hive/warehouse/tpcds_bin_partitioned_orc_10.db/web_returns<br>hdfs://HDP50/apps/hive/warehouse/tpcds_bin_partitioned_orc_10.db/web_sales<br>hdfs://HDP50/apps/hive/warehouse/tpcds_bin_partitioned_orc_10.db/web_site<br> | 


### Application Report

The process is complete with a markdown report location at the bottom of the output window.  You will need a *markdown* renderer to read the information.  Markdown is a text file, so if you don't have a renderer, you can still look at the report. It will just be harder to read.

The report location is displayed at the bottom of the application window when it's complete.

### Action Report for LEFT and RIGHT Clusters

Under certain conditions, additional actions may be required to achieve the desired 'mirror' state.  An output script for each cluster is created with these actions.  These actions are NOT run by `hms-mirror.`  They should be reviewed and understood by the owner of the dataset being `mirrored` and run when appropriate.

The locations are displayed at the bottom of the application window when it's complete.

### SQL Execution Output

A SQL script of all the actions taken will be written to the local output directory.

### Logs

There is a running log of the process in `$HOME/.hms-mirror/logs/hms-mirror.log.`  Review this for details or issues of the running process.

## Strategies

### Schema Only and DUMP

Transfer the schema only, replace the location with the RIGHT clusters location namespace, and maintain the relative path.

The data is transferred by an external process, like 'distcp.'

The DUMP strategy is like SCHEMA_ONLY; it just doesn't require the RIGHT cluster to be connected.  Although, it does need the following settings for the RIGHT cluster to make the proper adjustments:
```
legacyHive
hcfsNamespace
hiveServer2 -> partitionDiscovery 
```

When the option `-ma` (migrate acid) is specified, the ACID schema's will be migrated/dumped.  It is essential to know that the data for ACID tables can NOT simply be copied from one clusters hive instance to another.  The data needs to be extracted to a none ACID table, then use an external table definition to read and INSERT the data into the new ACID table on the RIGHT cluster.  For those that insist on trying to simply copy the data.... you've been warned ;).

With the DUMP strategy, you'll have a 'translated' (for legacy hive) table DDL that can be run on the new cluster independently.

[Sample Reports - SCHEMA_ONLY](./sample_reports/schema_only)

[Sample Reports - DUMP](./sample_reports/dump)

![schema_only](./images/schema_only.png)

![schema_only_cloud](./images/schema_only_cloud.png)

### LINKED

Assumes the clusters are [linked](#linking-clusters-storage-layers).  We'll transfer the schema and leave the location as is on the new cluster.

This provides a means to test hive on the RIGHT cluster using the LEFT cluster's storage.

The `-ma` (migrate acid) tables option is NOT valid in this scenario and will result in an error if specified.

[Sample Reports - LINKED](./sample_reports/linked)

![linked](./images/linked.png)

WARNING:  If the LOCATION element is specified in the database definition AND you use `DROP DATABASE ... CASCADE` from the RIGHT cluster, YOU WILL DROP THE DATA ON THE LEFT CLUSTER even though the tables are NOT purgeable.  This is the DEFAULT behavior of hive 'DROP DATABASE'.  So BE CAREFUL!!!!

### CONVERT_LINKED

If you migrated schemas with the [Linked](#linked) strategy and don't want to drop the database and run SCHEMA_ONLY to adjust the locations from the LEFT to the RIGHT cluster, use this strategy to make the adjustment.

This will work only on tables that were migrated already.  It will reset the location to the same relative location on the RIGHT clusters `hcfsNamespace`.  This will also check to see if the table was a 'legacy' managed table and set the `external.table.purge` flag appropriately.  Tables that are 'partitioned' will be handled differently, since each partition has a reference to the 'linked' location.  Those tables will first be validated that they NOT `external.table.purge`.  If they are, that property will 'UNSET'.  Then the table will be dropped, which will remove all the partition information.  Then they'll be created again and `MSCK` will be used to discover the partitions again on the RIGHT clusters namespace.

This process does NOT move data.  It expects that the data was migrated in the background, usually by `distcp` and has been placed in the same relative locations on the RIGHT clusters `hcfsNameSpace`.

This process does NOT work for ACID tables.

AVRO table locations and SCHEMA location and definitions will be changed and copied.

### SQL

We'll use SQL to migrate the data from one cluster to another.  The default behavior requires the clusters to be [linked](#linking-clusters-storage-layers).

![sql](./images/sql_exp-imp.png)

If the `-is <intermediate-storage-path>` is used with this option, we will migrate data to this location and use it as a transfer point between the two clusters.  Each cluster will require access (some configuration adjustment may be required) to the location.  In this scenario, the clusters do NOT need to be linked.

![intermediate](./images/intermediate.png)

### Export Import

We'll use EXPORT_IMPORT to get the data to the new cluster. The default behavior requires the clusters to be [linked](#linking-clusters-storage-layers).

EXPORT to a location on the LEFT cluster where the RIGHT cluster can pick it up with IMPORT.

When `-ma` (migrate acid) tables are specified, and the LEFT and RIGHT cluster DON'T share the same 'legacy' setting, we will NOT be able to use the EXPORT_IMPORT process due to incompatibilities between the Hive versions.  We will still attempt to migrate the table definition and data by copying the data to an EXTERNAL table on the lower cluster and expose this as the source for an INSERT INTO the ACID table on the RIGHT cluster.

![export_import](./images/sql_exp-imp.png)

If the `-is <intermediate-storage-path>` is used with this option, we will migrate data to this location and use it as a transfer point between the two clusters.  Each cluster will require access (some configuration adjustment may be required) to the location.  In this scenario, the clusters do NOT need to be linked.

![intermediate](./images/intermediate.png)

### Hybrid

Hybrid is a strategy that select either SQL or EXPORT_IMPORT for the
tables data strategy depending on the criteria of the table.

The `-ma|-mao` option is valid here and will use an internal `ACID` strategy for *transactional* tables to convert them between environments, along with their data.

Note: There are limits to the number of partitions `hms-mirror` will attempt.  See: [Partition Handling for Data Transfers](#partition-handling-for-data-transfers)

[Sample Reports - HYBRID](./sample_reports/hybrid)

![hybrid](./images/sql_exp-imp.png)

### Common

The data storage is shared between the two clusters, and no data migration is required.

Schemas are transferred using the same location.

[Sample Reports - COMMON](./sample_reports/common)

![common](./images/common.png)

### Storage Migration

See [Storage Migration](./strategy_docs/storage_migration.md)


## Troubleshooting / Issues

### Failed AVRO Table Creation



```
Error while compiling statement: FAILED: Execution Error, return code 40000 from org.apache.hadoop.hive.ql.ddl.DDLTask. java.lang.RuntimeException: MetaException(message:org.apache.hadoop.hive.serde2.SerDeException Encountered AvroSerdeException determining schema. Returning signal schema to indicate problem: Unable to read schema from given path: /user/dstreev/test.avsc)
```

#### Solution

Validate that the 'schema' file has been copied over to the new cluster.  If that has been done, check the permissions.  In a non-impersonation environment (doas=false), the `hive` user must have access to the file.

### Table processing completed with `ERROR.`

We make various checks as we perform the migrations, and when those checks don't pass, the result is an error.

#### Solution

In [tips](#tips-for-running-hms-miror) we suggest running with `dry-run` first (default).  This will catch the potential issues first, without taking a whole lot of time.  Use this to remediate issues before executing.

If the scenario that causes the `ERROR` is known, a remediation summary will be in the output report under **Issues** for that table.  Follow those instructions, then rerun the process with `--retry.` NOTE: `--retry` is currently tech preview and not thoroughly tested.

### Connecting to HS2 via Kerberos

Connecting to an HDP cluster running 2.6.5 with Binary protocol and Kerberos triggers an incompatibility issue:
`Unrecognized Hadoop major version number: 3.1.1.7.1....`

#### Solution

The application is built with CDP libraries (excluding the Hive Standalone Driver).  When `Kerberos is the `auth` protocol to connect to **Hive 1**, it will get the application libs which will NOT be compatible with the older cluster.

Kerberos connections are only supported to the CDP cluster.

When connecting via `Kerberos, you will need to include the `--hadoop-classpath` when launching `hms-mirror`.

### Auto Partition Discovery not working

I've set the `partitionDiscovery:auto` to `true,` but the partitions aren't getting discovered.

#### Solution

In CDP Base/PVC versions < 7.1.6 have not set the housekeeping thread that runs to activate this feature.

In the Hive metastore configuration in Cloudera Manager, set `metastore.housekeeping.threads.on=true` in the _Hive Service Advanced Configuration Snippet (Safety Valve) for hive-site.xml_

![pic](./images/hms_housekeeping_thread.png)

### Hive SQL Exception / HDFS Permissions Issues

```
Caused by: java.lang.RuntimeException: org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAccessControlException:Permission denied: user [dstreev] does not have [ALL] privilege on [hdfs://HDP50/apps/hive/warehouse/tpcds_bin_partitioned_orc_10.db/web_site]
```

This error is a permission error to HDFS.  For HYBRID, EXPORT_IMPORT, SQL, and SCHEMA_ONLY (with `-ams` enabled), this could be an issue with cross-cluster HDFS access.

Review the output report for details of where this error occurred (LEFT or RIGHT cluster).

When dealing with CREATE DDL statements submitted through HS2 with a `LOCATION` element in them, the submitting *user* **AND** the HS2 *service account* must have permissions to the directory.  Remember, with cross-cluster access, the user identity will originate on the RIGHT cluster and will be **EVALUATED** on the LEFT clusters storage layer.

For migrations, the `hms-mirror` running user (JDBC) and keytab user (HDFS) should be privileged users.

#### Example and Ambari Hints

After checking permissions of 'dstreev': Found that the 'dstreev' user was NOT the owner of the files in these directories on the LEFT cluster. The user running the process needs to be in 'dfs.permissions.superusergroup' for the lower clusters 'hdfs' service.  Ambari 2.6 has issues setting this property: https://jira.cloudera.com/browse/EAR-7805

Follow the workaround above or add the user to the 'hdfs' group. Or use Ranger to allow all access. On my cluster, with no Ranger, I had to use '/var/lib/ambari-server/resources/scripts/configs.py' to set it manually for Ambari.

`sudo ./configs.py --host=k01.streever.local --port=8080 -u admin -p admin -n hdp50 -c hdfs-site -a set -k dfs.permissions.superusergroup -v hdfs_admin`


### YARN Submission stuck in ACCEPTED phase

The process uses a connection pool to hive.  If the concurrency value for the cluster is too high, you may have reached the maximum ratio of AM (Application Masters) for the YARN queue.

Review the ACCEPTED jobs and review the jobs *Diagnostics* status for details on _why_ the jobs is stuck.

#### Solution

Either of:
1. Reduce the concurrency in the configuration file for `hms-mirror`
2. Increase the AM ratio or Queue size to allow the jobs to be submitted.  This can be done while the process is running.

### Spark DFS Access

If you have problems accessing HDFS from `spark-shell` or `spark-submit` try adding the following configuration to spark:

```
--conf spark.yarn.access.hadoopFileSystems=hdfs://<NEW_NAMESPACE>,hdfs://<OLD_NAMESPACE>
```

### Permission Issues

`HiveAccessControlException Permission denied user: [xxxx] does not have [ALL] privileges on ['location'] [state=42000,code=40000]`

and possibly

In HS2 Logs: `Unauthorized connection for super-user`

#### Solution

Caused by the following:
- The 'user' doesn't have access to the location as indicated in the message.  Verify through 'hdfs' that this is true or not. If the user does NOT have access, grant them access and try again.
- The 'hive' service account running HS2 does NOT have access to the location.  The message will mask this and present it as a 'user' issue, when it is in fact an issue with the 'hive' service account.  Grant the account the appropriate access.
- The 'hive' service does NOT have proxy permissions to the storage layer.
  - Check the `hadoop.proxyuser.hive.hosts|groups` setting in `core-site.xml`.  If you are running into this `super-user` error on the RIGHT cluster, while trying to access a storage location on the *LEFT* cluster, ensure the proxy settings include the rights values in the RIGHT clusters `core-site.xml`, since that is where HS2 will pick it up from.

### Must use HiveInputFormat to read ACID tables

We've seen this while attempting to migrate ACID tables from older clusters (HDP 2.6).  The error occurs when we try to extract the ACID table data to a 'transfer' external table on the LEFT cluster, which is 'legacy'.

#### Solution

HDP 2.6.5, the lowest supported cluster version intended for this process, should be using the 'tez' execution engine  `set hive.execution.engine=tez`.  If the cluster has been upgraded from an older HDP version OR they've simply decided NOT to use the `tez` execution engine', you may get this error.

In `hms-mirror` releases 1.3.0.5 and above, we will explicitly run `set hive.execution.engine=tez` on the LEFT cluster when identified as a 'legacy' cluster.  For version 1.3.0.4 (the first version to support ACID transfers), you'll need to set the hive environment for the HS2 instance you're connecting to use `tez` as the execution engine.

### ACL issues across cross while using LOWER clusters storage

Are you seeing something like this?

```aidl
org.apache.hadoop.hive.ql.ddl.DDLTask. MetaException(message:Got exception: org.apache.hadoop.security.AccessControlException Permission denied: user=hive, access=WRITE, inode="/apps/hive/warehouse/merge_files.db/merge_files_part_a_small_replacement":dstreev:hadoop:drwxr-xr-x at
```

This is caused when trying to `CREATE` a table on the **RIGHT** cluster that references data on the **LEFT** cluster.  When the LEFT cluster is setup differently with regard to impersonation (doas) than the RIGHT, transfer tables are created with POSIX permissions that may not allow the RIGHT cluster/user to access that location.

#### Solution

Using Ranger on the LEFT cluster, open up the permissions to allow the requesting user access as identified.

