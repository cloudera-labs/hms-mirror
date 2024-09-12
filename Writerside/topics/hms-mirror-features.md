# Features

`hms-mirror` is designed to migrate schema definitions from one cluster to another or simply provide an extract of the schemas via `-d DUMP`.

Under certain conditions, `hms-mirror` will 'move' data too.  Using the data strategies `-d SQL|EXPORT_IMPORT|HYBRID` well use a combination of SQL temporary tables and [Linking Clusters Storage Layers](Linking-Cluster-Storage-Layers.md) to facilitate this.

## Iceberg Table Migration via Hive

See [Iceberg Migration](hms-mirror-iceberg_migration.md) for details.

## File System Stats

SQL based operations, `hms-mirror` will attempt to gather file system stats for the tables being migrated.  This is done by running `hdfs dfs -count` on the table location.  This is done to help determine the best strategy for moving data and allows us to set certain hive session values and distribution strategies in SQL to optimize the data movement.

But some FileSystems may not be very efficient at gathering stats.  For example, S3.  In these cases, you can disable the stats gathering by adding `-ssc|--skip-stats-collection` to your command line.

When you have a LOT of tables, collecting stats can have a significant impact on the time it takes to run `hms-mirror` and the general pressure on the FileSystem to gather this information.  In this case, you have to option to disable stats collection through `-scc`.

## CREATE [EXTERNAL] TABLE IF NOT EXISTS Option

Default behavior for `hms-mirror` is to NOT include the `IF NOT EXISTS` clause in the `CREATE TABLE` statements.  This is because we want to ensure that the table is created and that the schema is correct.  If the table already exists, we want to fail.

But there are some scenarios where the table is present and we don't want the process to fail on the CREATE statement to ensure the remaining SQL statements are executed.  In this case, you can modify add the commandline option `-cine` or
add to the configuration:

```yaml
  clusters:
    RIGHT|LEFT:
      createIfNotExists: "true"
```

Using this option has the potential to create a table with a different schema than the source.  This is not recommended.  This option is applied when using the `SCHEMA_ONLY` data strategy.

## Auto Gathering Stats (disabled by default)

CDP Default settings have enabled `hive.stats.autogather` and `hive.stats.column.autogather`.  This impacts the speed of INSERT statements (used by `hms-mirror` to migrate data) and for large/numerous tables, the impact can be significant.

The default configuration for `hms-mirror` is to disable these settings.  You can re-enable this in `hms-mirror` for each side (LEFT|RIGHT) separately.  Add the following to your configuration.

```yaml
clusters:
  LEFT|RIGHT:
    enableAutoTableStats: true
    enableAutoColumnStats: true
```

To disable, remove the `enableAutoTableStats` and `enableAutoColumnStats` entries or set them to `false`.

## Non-Standard Partition Locations

Partitions created by 'hive' with default locations follow a file system naming convention that allows other partitions of 'hive' to discovery/manage those location and partition associations.

The standard is for partitions to exist as sub-directories of the table location.  For example: Table Location is `hdfs://my-cluster/warehouse/tablespace/external/hive/my_test.db/my_table` and the partition location is `hdfs://my-cluster/warehouse/tablespace/external/hive/my_test.db/my_table/dt=2020-01-01`, assuming the partition column name is `dt`.

When this convention is not followed, additional steps are required to build the partition metadata.  You can't use `MSCK REPAIR` because it will not find the partitions.  You can use `ALTER TABLE ADD PARTITION` but you'll need to provide the location of the partition.  `hms-mirror` will do this for you when using the data strategies `-d DUMP|SCHEMA_ONLY` and the commandline flag `-epl|--evaluate-partition-location`.

In order to make this evaluation efficient, we do NOT use standard HiveSQL to discover the partition details.  It is possible to use HiveSQL for this, it's just not meant for operations at scale or tables with a lot of partitions.

Hence, we tap directly into the hive metastore database.  In order to use this feature, you will need to add the following configuration definition to your hms-mirror configuration file (default.yaml).

```yaml
clusters:
  LEFT|RIGHT:
    ...
    metastore_direct:
      uri: "<db_url>"
      type: MYSQL|POSTGRES|ORACLE
      connectionProperties:
        user: "<db_user>"
        password: "<db_password>"
      connectionPool:
        min: 3
        max: 5
```

You will also need to place a copy of the RDBMS JDBC driver in `$HOME/.hms-mirror/aux_libs`.  The driver must match the `type` defined in the configuration file.

**Note: Non-Standard Partition Location will affect other strategies like `SQL` where the LEFT clusters storage is accessible to the RIGHT and is used by the RIGHT to source data. The 'mirror' table used for the transfer will NOT discover the partitions and will NOT transfer data.  See: [Issue #63](https://github.com/cloudera-labs/hms-mirror/issues/63) for updates on addressing this scenario.  If this is affecting you, I highly recommend you comment on the issue to help us set priorities.**

## Optimizations

The following configuration settings control the various optimizations taken by `hms-mirror`. These settings are mutually exclusive.

- `-at|--auto-tune`
- `-so|--skip-optimizations`
- `-sdpi|--sort-dynamic-partition-inserts`

### Auto-Tune

`-at|--auto-tune`

Auto-tuning will use some basic file level statistics about tables/partitions to provide overrides for the following settings:

- `tez.grouping.max-size`
- `hive.exec.max.dynamic.partitions`
- `hive.exec.reducers.max`

in addition to these session level setting, we'll use those basic file statistics to construct migration scripts that address things like 'small-files' and 'large' partition datasets.

We'll set `hive.optimize.sort.dynamic.partition.threshold=-1` and append `DISTRIBUTE BY` to the SQL migration sql statement, just like we do with `-sdpi`.  But we'll go one step further and review the average partition size and add an additional 'grouping' element to the SQL to ensure we get efficient writers to a partition.  The means that tables with large partition datasets will have more than the standard single writer per partition, preventing the LONG running hanging task that is trying to write a very large partition.

### Sort Dynamic Partition Inserts

`-sdpi|--sort-dynamic-partition-inserts`

This will set the session property `hive.optimize.sort.dynamic.partition.threshold=0`, which will enable plans to distribute multi partition inserts by the partition key, therefore reducing partitions writes to a single 'writer/reducer'.

When this isn't set, we set `hive.optimize.sort.dynamic.partition.threshold=-1`, and append `DISTRIBUTE BY` to the SQL migration sql statement to ensure the same behavior of grouping reducers by partition values.

### Skip Optimizations

`-so`

[Feature Request #23](https://github.com/cloudera-labs/hms-mirror/issues/23) was introduced in v1.5.4.2 and give an option to **Skip Optimizations**.

When migrating data via SQL with partitioned tables (OR downgrading an ACID table), there are optimizations that we apply to help hive distribute data more efficiently.  One method is to use `hive.optimize.sort.dynamic.partition=true` which will "DISTRIBUTE" data along the partitions via a Reduction task.  Another is to declare this in SQL with a `DISTRIBUTE BY` clause.

But there is a corner case where these optimizations can get in the way and cause long-running tasks.  If the source table has already been organized into large files (which would be within the partitions already), adding the optimizations above force a single reducer per partition.  If the partitions are large and already have good file sizes, we want to skip these optimizations and let hive run the process with only a map task.

## HDP3 MANAGEDLOCATION Database Property

[HDP3 doesn't support MAANGEDLOCATION](https://github.com/cloudera-labs/hms-mirror/issues/52) so we've added a property to the cluster configuration to allow the system to *SKIP* setting the `MANAGEDLOCATION` database property in HDP 3 / Hive 3 environments.

```yaml
clusters:
  LEFT:
    platformType: 'HDP3'
```

## Compress Text Output

`-cto` will control the session level setting for `hive.exec.compress.output'.

## VIEWS

`hms-mirror` now supports the migration of VIEWs between two environments.  Use the `-v|--views-only` option to execute this path.  VIEW creation requires dependent tables to exist.

Run `hms-mirror` to create all the target tables before running it with the `-v` option.

This flag is an `OR` for processing VIEW's `OR` TABLE's.  They are NOT processed together.

**Requirements**

- The dependent tables must exist in the RIGHT cluster
- When using `-dbp|--db-prefix` option, VIEW definitions are NOT modified and will most likely cause VIEW creation to fail.

## ACID Tables

`hms-mirror` supports the migration of ACID tables using the `-d HYBRID|SQL|EXPORT_IMPORT` data strategy in combination with the `-ma|--migrate-acid` or `-mao|--migrate-acid-only` flag.   You can also simply 'replay' the schema definition (without data) using `-d SCHEMA_ONLY -ma|-mao`.  The `-ma|-mao` flag takes an *optional* integer value that sets an 'Artificial Bucket Threshold'.  When no parameter is specified, the default is `2`.

Use this value to set a bucket limit where we'll *remove* the bucket definition during the translation.  This is helpful for legacy ACID tables which *required* a bucket definition but weren't a part of the intended design.  The migration provides an opportunity to correct this artificial design element.

With the default value `2`, we will *remove* CLUSTERING from any ACID table definitions with `2` or fewer buckets defined.  If you wish to keep ALL CLUSTERED definitions, regardless of size, set this value to `0`.

There is now an option to 'downgrade' ACID tables to EXTERNAL/PURGE during migration using the `-da` option.

### The ACID Migration Process

The ACID migration builds a 'transfer' table on the LEFT cluster, a 'legacy' managed table (when the LEFT is a legacy cluster), or an 'EXTERNAL/PURGE' table.  Data is copied to this transfer table from the original ACID table via SQL.

Since the clusters are [linked](Linking-Cluster-Storage-Layers.md), we build a 'shadow' table that is 'EXTERNAL' on the 'RIGHT' cluster that uses the data in the 'LEFT' cluster.  Similar to the LINKED data strategy.  If the data is partitioned, we run `MSCK` on this 'shadow' table in the 'RIGHT' cluster to discover all the partitions.

The final ACID table is created in the 'RIGHT' cluster, and SQL is used to copy data from the 'LEFT' cluster via the 'shadow' table.

**Requirements**

- Data Strategy: `HYBRID`, `SQL`, or `EXPORT_IMPORT`
- Activate Migrate ACID: `-ma|-mao`
- [Link Clusters](Linking-Cluster-Storage-Layers.md), unless using the `-is|--intermediate-storage` option.
- This is a 'ONE' time transfer.  It is not an incremental update process.
- Adequate Storage on LEFT to make an 'EXTERNAL' copy of the ACID table.
- Permissions:
    - From the RIGHT cluster, the submitting user WILL need access to the LEFT cluster's storage layer (HDFS) to create the shadow table (with location) that points across clusters.
    - doas will have a lot to do with the permissions requirements.
    - The 'hive' service account on the RIGHT cluster will need elevated privileges to the LEFT storage LAYER (HDFS).  For example: If the hive service accounts on each cluster DO NOT share the same identity, like `hive`, then the RIGHT hive identity MUST also have privileged access to the LEFT clusters HDFS layer.
- Partitioned tables must have data that is 'discoverable' via `MSCK`.
  NOTE: The METADATA activity and REDUCER restrictions to the number of BUCKETs can dramatically affect this.- The number of partitions in the source ACID tables must be below the `partitionLimit` (default 500).  This strategy may not be successful when the partition count is above this, and we won't even attempt the conversion. Check YARN for the progress of jobs with a large number of partitions/buckets.  Progress many appear stalled from 'hms-mirror'.
- ACID table migration to Hive 1/2 is NOT supported due to the lack of support for "INSERT OVERWRITE" on transactional tables.  Hive 1/2 to Hive 3 IS support and the target of this implementation.  Hive 3 to Hive 3 is also supported.

### Replace ACID `-r` or `--replace`

When downgrading ACID tables during migration, the `-r` option will give you the option to 'replace' the original ACID table with the a table that is no longer ACID.  This option is only available along with the `-da` and `SQL` data strategy options.

## Intermediate/Common Storage Options

When bridging the gap between two clusters, you may find they can't share/link storage. In this case, using one of these options will help you with the transfer.

The `-is` or `--intermediate-storage` option is consider a transient location that both cluster can share, see, and have access to.  The strategies for transferring data (EXPORT_IMPORT, SQL, HYBRID) will use this location to facilitate the transfer.  This is a common strategy when migrating from on-prem environments to the cloud.

The `-cs` or `--common-storage` option is similar to `-is` but this option ends up being the final resting place for the data, not just the transfer location.  And with this option, we can streamline the jumps required to migrate data.  Again, this location needs to be accessible to both clusters.


## Non-Native Hive Tables (Hbase, KAFKA, JDBC, Druid, etc..)

Any table definition without a `LOCATION` element is typically a reference to an external system like: HBase, Kafka, Druid, and/or (but not limited to) JDBC.

**Requirements**

These references require the environment to be:
- Correctly configured to use these resources
- Include the required libraries in the default hive environment.
- The referenced resource must exist already BEFORE the 'hive' DDL will successfully run.

## AVRO Tables

AVRO tables can be designed with a 'reference' to a schema file in `TBLPROPERTIES` with `avro.schema.url`.  The referenced file needs to be 'copied' to the *RIGHT* cluster BEFORE the `CREATE` statement for the AVRO table will succeed.

Add the `-asm|--avro-schema-move` option at the command line to *copy* the file from the LEFT cluster to the RIGHT cluster.

As long as the clusters are [linked](Linking-Cluster-Storage-Layers.md) and the cluster `hcfsNamespace` values are accurate, the user's credentials running `hms-mirror` will attempt to copy the schema file to the *RIGHT* cluster BEFORE executing the `CREATE` statement.

**Requirements**

- [Link Clusters](Linking-Cluster-Storage-Layers.md) for Data Strategies: `SCHEMA_ONLY`, `SQL`, `EXPORT_IMPORT`, and `HYBRID`
- Running user must have 'namespace' access to the directories identified in the `TBLPROPERTIES` key `avro.schema.url`.
- The user running `hms-mirror` will need enough storage level permissions to copy the file.
- When hive is running with `doas=false`, `hive` will need access to this file.

### Warnings

- With the `EXPORT_IMPORT` strategy, the `avro.schema.url` location will NOT be converted. It may lead to an issue reading the table if the location includes a prefix of the cluster's namespace OR the file doesn't exist in the new cluster.

## Table Translations

### Legacy Managed Tables
`hms-mirror` will convert 'legacy' managed tables in Hive 1 or 2 to EXTERNAL tables in Hive 3.  It relies on the `legacyHive` setting in the cluster configurations to accurately make this conversion.  So make sure you've set this correctly.

## `distcp` Planning Workbook and Scripts

`hms-mirror` will create source files and a shell script that can be used as the basis for the 'distcp' job(s) used to support the databases and tables requested in `-db`.  `hms-mirror` will NOT run these jobs.  It will provide the basic job constructs that match what it did for the schemas.  Use these constructs to build your execution plan and run these separately.

The constructs created are intended as a *one-time* transfer.  If you are using *SNAPSHOTS* or `--update` flags in `distcp` to support incremental updates, you will have to make additional modifications to the scripts/process.  Note: For these scenarios, `hms-mirror` supports options like `-ro|--read-only` and `-sync`.

Each time `hms-mirror` is run, *source* files for each database are created.  These source files need to be copied to the distributed filesystem and reference with an `-f` option in `distcp`.  We also create a *basic* shell script that can be used as a template to run the actual `distcp` jobs.

Depending on the job size and operational expectations, you may want to use *SNAPSHOTS* to ensure an immutable source or use a `diff` strategy for more complex migrations.  Regardless, you'll need to make modifications to these scripts to suit your purposes.

If your process requires the data to exist BEFORE you migrate the schemas, run `hms-mirror` in the `dry-run` mode (default) and use the distcp planning workbook and scripts to transfer the datasets.  Then run `hms-mirror` with the `-e|--execute` option to migrate the schemas.

These workbooks will NOT include elements for ACID/Transactional tables.  Simply copying the dataset for transactional tables will NOT work.  Use the `HYBRID` data strategy migration transactional table schemas and datasets.

## ACID Table Downgrades

The default table creation scenario for Hive 3 (CDP and HDP 3) installations is to create ACID transactional tables.  If you're moving from a legacy platform like HDP 2 or CDH, this may have caught you off guard and resulted in a lot of ACID tables you did NOT intend on.

The `-da|--downgrade-acid` option can be used to convert these ACID tables to *EXTERNAL/PURGE* tables.

If you have ACID tables on the current platform and would like to *downgrade* them, but you're not doing a migration, try the `-ip|--in-place` option.  This will archive to existing ACID table and build a new table (with the original table name) that is *EXTERNAL/PURGE*.

## Reset to Default Locations

Migrations present an opportunity to clean up a lot of history.  While `hms-mirror` was originally designed to migration data and maintain the **relative** locations of the data, some may want to reorganize the data during the migration.

The option `-rdl|--reset-default-location` will overwrite the locations originally used and place the datasets in the 'default' locations as defined by `-wd|--warehouse-directory` and `-ewd|--external-warehouse-directory`.

The `-rdl` option requires `-wd` and `-ewd` to be specified.  These locations will be used to `ALTER` the databases `LOCATION` and `MANAGEDLOCATION` values.  After which, all new `CREATE \[EXTERNAL\] TABLE` definitions don't specify a `LOCATION`, which means table locations will use the default.

## Legacy Row Serde Translations

By default, tables using old row *serde* classes will be converted to the newer serde as the definition is processed by `hms-mirror`.  See [here](https://github.com/cloudera-labs/hms-mirror/blob/6f6c309f24fbb8133e5bd52e5b18274094ff5be8/src/main/java/com/cloudera/utils/hadoop/hms/mirror/feature/LegacyTranslations.java#L28) for a list of serdes we look for.

If you do NOT want to apply this translation, add the option `-slt|--skip-legacy-translation` to the commandline.

## Filtering Tables to Process

There are options to filter tables included in `hms-mirror` process.  You can select `-tf|--table-filter` to "include" only tables that match this 'regular-expression'.  Inversely, use `-etf|--exclude-table-filter` to omit tables from the list.  These options are mutually exclusive.

The filters for `-tf` and `-tef` are expressed as a 'regular-expression'.  Complex expressions should be enclosed in quotes to ensure the commandline interpreter doesn't split them.

Additional table filters (`-tfs|--table-filter-size-limit` and `-tfp|--table-filter-partition-count-limit`) that check a tables data size and partition count limits can also be applied to narrow the range of tables you'll process.

The filter does NOT override the requirement for options like `-ma|-mao`.  It is used as an additional filter.

## Migrations between Clusters WITHOUT line of Site

There will be cases where clusters can't be [linked](Linking-Cluster-Storage-Layers.md).  And without this line of sight, data movement needs to happen through some other means.

### On-Prem to Cloud
This scenario is most common with "on-prem" to "cloud" migrations.  Typically, `hms-mirror` is run from the **RIGHT** cluster, but in this case the **RIGHT** cloud cluster doesn't have line of site to the **LEFT** on-prem cluster.  But the on-prem cluster will have limited line of site to the cloud environment.  In this case, the only option is to run `hms-mirror` from the on-prem cluster.  The on-prem cluster will need access to either an `-is|--intermediate-storage` location that both clusters can access or a `-cs|--common-storage` location that each cluster can see, but can be considered the final resting place for the cloud environment.  The `-cs` option usually means there are fewer data hops required to complete the migration.

You'll run `hms-mirror` from a **LEFT** cluster edgenode.  This node will require line of site to the Hive Server 2 endpoint in the **RIGHT** cloud environment.  The **LEFT** cluster will also need to be configured with the appropriate libraries and configurations to write to the location defined in `-is|-cs`.  For example: For S3, the **LEFT** cluster will need the AWS S3 libraries configured and the appropriate keys for S3 access setup to complete the transfer.

## Shared Storage Models (Isilon, Spectrum-Scale, etc.)

There are cases where 'HDFS' isn't the primary data source.  So the only thing the cluster share is storage in these 'common' storage units.  You want to transfer the schema, but the data doesn't need to move (at least for 'EXTERNAL' (non-transactional) tables).  In this case, try the `-d|--data-strategy` COMMON.  The schema's will go through all the needed conversions while the data remains in the same location.

## Disconnected Mode

Use the `-rid|--right-is-disconnected` mode when you need to build (and/or) transfer schema/datasets from one cluster to another, but you can't connect to both at the same time.  See the issues log for details regarding the cases here [issue #17](https://github.com/cloudera-labs/hms-mirror/issues/17).

Use cases:
- Schema Only Transfers
- SQL, EXPORT_IMPORT, and HYBRID only when -is or -cs is used. This might be the case when the clusters are secure (kerberized), but don't share a common kerberos domain/user auth. So an intermediate or common storage location will be used to migrate the data.
- Both clusters (and HS2 endpoints) are Kerberized, but the clusters are NOT the same major hadoop version. In this case, hms-mirror doesn't support connecting to both of these endpoints at the same time. Running in the disconnected mode will help push through with the conversion.

hms-mirror will run as normal, with the exception of examining and running scripts against the right cluster. It will be assumed that the RIGHT cluster elements do NOT exist.

The RIGHT_ 'execution' scripts and distcp commands will need to be run MANUALLY via Beeline on the RIGHT cluster.

Note: This will be know as the "right-is-disconnected" option. Which means the process should be run from a node that has access to the "left" cluster. This is 'counter' to our general recommendation that the process should be run from the 'right' cluster.

## No-Purge Option

`-np`

[Feature Request #25](https://github.com/cloudera-labs/hms-mirror/issues/25) was introduced in v1.5.4.2 and gives the user to option to remove the `external.table.purge` option that is added when converting legacy managed tables to external table (Hive 1/2 to 3).  This does affect the behavior of the table from the older platforms.

## Property Overrides

`-po[l|r] <key=value>[,<key=value>]...`

[Feature Request #27](https://github.com/cloudera-labs/hms-mirror/issues/27) introduced in v1.5.4.2 provides the ability to set a hive properties at the beginning of each migration part.  This is a comma separated list of key=value pairs  with no space.  If spaces are needed, quote the parameter on the commandline.

You can use `-po` to set the properties for BOTH clusters or `-pol`|`-por` to set them specifically for the 'left' and/or 'right' cluster.

For example: `-po hive.exec.orc.split.strategy=BI,hive.compute.query.using.stats=false`

To provide a consistent list of settings for ALL jobs, add/modify the following section in the configuration file ie: `default.yaml` used for processing.  In this case you do NOT need to use the commandline option.  Although, you can set basic values in the configuration file and add other via the commandline.

Notice that there are setting for the LEFT and RIGHT clusters.

```yaml
optimization:
  overrides:
    left:
      tez.queue.name: "compaction"
    right:
      tez.queue.name: "migration"
      
```

## Global Location Map

`-glm|--global-location-map <from=to>[,...]`

This is an opportunity to make some specific directory mappings during the migration.  You can supply a comma separated list of directory pairs to be use for evaluation.

`-glm /data/my_original_location=/corp/finance_new_loc,/user/hive=/warehouse/hive`

These directory mappings ONLY apply to 'EXTERNAL' and 'DOWNGRADED ACID' tables.  You can supply 'n' number of mappings to review through the commandline interface as describe above.  To provide a consistent set of mappings to ALL jobs, add/modify the following section in the configuration file ie: `default.yaml` used for processing.

```yaml
translator:
  globalLocationMap:
    /data/my_original_location: "/corp/finance_new_loc"
    /user/hive: "/warehouse/hive"
```

The list will be sorted by the length of the string, then alpha-numerically.  This will ensure the deepest nested paths are evaluated FIRST. When a path is matched during evaluation, the process will NOT look and any more paths in the list.  Therefore, avoiding possible double evaluations that may result when there are nested paths in the list.

Paths are evaluated with 'startsWith' on the original path (minus the original namespace). When a match is found, the path 'part' will be replaced with the value specified.  The remaining path will remain intact and regardless of the `-rdl` setting, the LOCATION element will be included in the tables new CREATE statement.

## Force External Locations

`-fel|--force-external-location`

Under some conditions, the default warehouse directory hierarchy is not honored.  We've seen this in HDP 3.  The `-rdl` option collects the external tables in the default warehouse directory by omitting the LOCATION element in the CREATE statement, relying on the default location.  The default location is set at the DATABASE level by `hms-mirror`.

In HDP3, the CREATE statement doesn't honor the 'database' LOCATION element and reverts to the system wide warehouse directory configurations.  The `-fel` flag will simply include the 'properly' adjusted LOCATION element in the CREATE statement to ensure the tables are created in the desired location.  This setting overrides the effects intended by the `-rdl` option which intend to place the tables under the stated warehouse locations by omitting the location from the tables definition and relying on the default location specified in the database.

`-fel` will use the original location as a starting point.  If `-wd|-ewd` are specified, they aren't not used in the translation, but warnings may be issued if the final location doesn't align with the warehouse directory.  The effect change in the location when using `-fel`, add mappings via `-glm`.

## HDP 3 Hive

HDP 3 (Hive 3) was an incomplete implementation with regards to complex table 'location' management.  When you are working in this environment, add to the cluster configuration (LEFT or RIGHT) the setting: `hdpHive3: true`.  There is NOT a commandline switch for this.  JUst add it to the configuration file you're using to run the application.

```yaml
clusters:
  LEFT:
    environment: "LEFT"
    legacyHive: false
    hdpHive3: true
```

HDP3 Hive did NOT have a MANAGEDLOCATION attribute for Databases.

The LOCATION element tracked the Manage ACID tables and will control where they go.

This LOCATION will need to be transferred to MANAGEDLOCATION 'after' upgrading to CDP to ensure ACID tables maintain the same behavior.  EXTERNAL tables will explicity set there LOCATION element to match the setting in `-ewd`.

Future external tables, when no location is specified, will be created in the `hive.metastore.warehouse.external.dir`.  This value is global in HDP Hive3 and can NOT be set for individual databases.

Post upgrade to CDP, you should add a specific directory value at the
database level for better control.

## Schema Fix Features

Schema Fix Features are a way to inject special considerations into the replay of a schema between clusters.  Each schema is automatically check is a particular 'feature' applies.

If you find that this features check is causing issues, add the flag `-sf` to the application parameters and the feature checks will be skipped.

### BAD_ORC_DEF

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

### BAD_RC_DEF

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

### BAD_TEXTFILE_DEF

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
