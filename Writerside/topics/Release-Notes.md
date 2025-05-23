# Release Notes

## Known Issues

The latest set of known issues can be found [here](https://github.com/cloudera-labs/hms-mirror/issues?q=is%3Aissue+is%3Aopen+label%3Abug)

## Enhancement Requests

The latest set of enhancement requests can be found [here](https://github.com/cloudera-labs/hms-mirror/issues?q=is%3Aissue+is%3Aopen+label%3Aenhancement). 

If there is something you'd like to see, add a new issue [here](https://github.com/cloudera-labs/hms-mirror/issues)

## 3.1.0.1

**What's New**
Better connection and connection pooling property management, validation, and configuration.  See [JDBC Connections](JDBC-Drivers-and-Configuration.md#valid-jdbc-driver-parameters-v3-1-0-1)

## 3.1.0.0

**What's New**
Upgraded `hadoop-common` to 3.4.1 for JDK17 compile-time compatibility. This avoids the need to `add-exports` for the `java.naming` module.

## 3.0.0.2

**Bug Fixes**
[--sync not dropping table on right when left is missing. #189](https://github.com/cloudera-labs/hms-mirror/issues/189)

[When doing a Schema Sync for 'VIEWS', there aren't being dropped. #190](https://github.com/cloudera-labs/hms-mirror/issues/190)

[SHADOW table schema for partitioned tables are creating ALTER TABLE ... PARTITION location details with LEFT locations. #191](https://github.com/cloudera-labs/hms-mirror/issues/191)

[Add ability to save a comment for a run and record it in the report. #192](https://github.com/cloudera-labs/hms-mirror/issues/192)

[Themeleaf syntax causing excessive logging. #193](https://github.com/cloudera-labs/hms-mirror/issues/193)

- Removed Spring Autowiring
- Function Documentation

## 3.0.0.1

This release is based on the 2.3.1.5 release and includes all the features and bug fixes from that release.

This is a Security and CVE release that has upgrading all dependencies to the latest possible versions to eliminate 
as many of the community CVEs as possible.  This also required us to upgrade the minimum JDK version to 17.

**What's New**
- JDK 17 Minimum Version Requirement. Addresses dependencies with CVE issues.

**Bug Fixes**
[--sync not dropping table on right when left is missing.](https://github.com/cloudera-labs/hms-mirror/issues/189)

## 2.3.1.5

**Bug Fixes**
- Fixed Web UI session status preventing progress.
- Handle npe from SQL in-place downgrade of ACID tables.
- Fixed locale issue with set statements that used numeric values.
- Fixed floorDiv(long,int) to floorDiv(long,long) for Java8 compatibility.

**What's New**
- Add support to 'in-place' removal of bucket definitions from an ACID table

**Note**
This will be the last release in the 2.3.x branch with any feature enhancements. Future releases will be in the 3.x 
branch.

## 2.3.0.13

**What's New**

[Enhance logging to show which instance is handling a connection/job in case of using multiple HS2 instances](https://github.com/cloudera-labs/hms-mirror/issues/183)
> When using multiple HS2 instances, it can be difficult to determine which instance is handling a connection or job.  This enhancement will allow you to add a `set` statement that identifies which instance is handling the connection or job. Use the `-po[r|l]` property override values if you'd like to include this information in the log output. `-po hive.server2.thrift.bind.host` will add the statement to both LEFT and RIGHT output. The option is also available in the Web UI through "Property Overrides".

## 2.3.0.12

**Bug Fixes**

[The Hikari Connection Pool settings are causing intermittent connection failures, cause table transfer failures.](https://github.com/cloudera-labs/hms-mirror/issues/182)


## 2.3.0.10

**Bug Fixes**

[Second run in WebUI Fails](https://github.com/cloudera-labs/hms-mirror/issues/181)

[dbRegEx not being processed. Throws MISC_ERROR because it can't find any databases.](https://github.com/cloudera-labs/hms-mirror/issues/180)

[Legacy DBPROPERTIES are causing ERROR when attempting to set on CDP](https://github.com/cloudera-labs/hms-mirror/issues/179)

Issue with jobs not completing when some schema's were already present.

Address lingering connections after run completes.

Fixed counters for CLI screen output.

Fixed an issue with tables being processed multiple times under some conditions.

[Partition discovery for SHADOW table when source is a Managed table shouldn't try to build partitions with ALTER](https://github.com/cloudera-labs/hms-mirror/issues/178)

[LEFT side SQL when running 'execute' mode for SQL data strategy isn't being run.](https://github.com/cloudera-labs/hms-mirror/issues/177)

[CLI App version fails when attempting to set 'concurrency' option](https://github.com/cloudera-labs/hms-mirror/issues/176)

[MSCK for Shadow table not generated when 'metastore_direct' on the LEFT isn't defined](https://github.com/cloudera-labs/hms-mirror/issues/175)


## 2.3.0.4

**What's New**

BETA Iceberg conversion support for the STORAGE_MIGRATION data strategy. See [Iceberg Conversion](hms-mirror-iceberg_migration.md) for more details. To activate this beta feature for the WebUI, add `--hms-mirror.
config.beta=true` to the startup command.  EG: `hms-mirror --service --hms-mirror.config.beta=true`

**Bugs (Fixed)**

[Add to connection init the ability to set the queue AND trigger an engine resource](https://github.com/cloudera-labs/hms-mirror/issues/173)

[Validate SQL elements before making changes to the Cluster](https://github.com/cloudera-labs/hms-mirror/issues/168)

[Extend the HS2 connection validation with Tez task validation](https://github.com/cloudera-labs/hms-mirror/issues/167)

[The reset-to-default-location doesn't seem to be working in v2](https://github.com/cloudera-labs/hms-mirror/issues/165)


## 2.2.0.19.6 (pre-release)

**What's New**

[BEHAVIOR CHANGE - Drop any shadow table definitions created during migration automatically](https://github.com/cloudera-labs/hms-mirror/issues/163)
> This adjustment will remove shadow tables that were created during the migration process.  This will help to keep 
> the source and target clusters clean of any artifacts created during the migration process. An option 
> `saveWorkingTables` has been added to the configuration to allow you to keep these tables if you need them for any 
> audits or other reasons. The default is `false`, which means that the tables will be dropped automatically. An audit 
> 'cleanup' file with the drop commands will be created in the report directory, regardless of the setting.

## 2.2.0.19.2 (pre-release)

**What's New**

[Error event is not logged for table skipped because RIGHT already has the table](https://github.com/cloudera-labs/hms-mirror/issues/162)
[Would be great to be able to omit warnings from the end of logs](https://github.com/cloudera-labs/hms-mirror/issues/161)
[Be able to reduce spring framework related log entries](https://github.com/cloudera-labs/hms-mirror/issues/160)

## 2.2.0.19.1

**Bug Fixes**

- Doubling of mapping locations for partitions in `distcp` for STORAGE_MIGRATION.
- [STORAGE_MIGRATION is setting ACID to ON, regardless.](https://github.com/cloudera-labs/hms-mirror/issues/158)

**What's New**

- [Validate JDBC Jar Files in config.](https://github.com/cloudera-labs/hms-mirror/issues/159)
- Ability to turn-on `strict` mode for Storage Migration.  This will cause `distcp` to fail when non-standard locations are used.  To turn off, use the `-sms|--storage-migration-strict` flag via the CLI.

**Behavior Changes**

The default behavior for Storage Migration 'strict' has changed from `true` to `false`. The intent behind the `strict` mode was to ensure `distcp` would fail when non-standard locations are used.  The combination of `metastore_direct` and knowing the partition location details gives us a better chance on making these mappings work for `distcp`.  When the scenario arises, we do **HIGHLY** recommend that you validate the plans created.  The new default behavior will allow `distcp` to continue when non-standard locations are encountered, while throwing a warning.  This will allow the migration to continue, but you should validate the results.

<warning>
You will need to reset the `strict` mode flag in you configuration yaml.  It will be set to `true` if the configuration was created before this release.  You will need to set it to `false` to maintain this new 'preferred' behavior.

```yaml
transfer:
  storageMigration:
    strict: false
```
</warning>

## 2.2.0.18.1


**What's New**

Beta Flag to be used for future beta features.  To activate:

<tabs>
<tab title="CLI">

```shell
hms-mirror --beta
```

</tab>
<tab title="Web UI">

```shell
hms-mirror --service --hms-mirror.config.beta=true
```

</tab>
</tabs>

**Bugs (Fixed)**

- [Property overrides - po|pol|por - are not honored in v2, were working in 1.6.x](https://github.com/cloudera-labs/hms-mirror/issues/156)
- [Partition auto-discovery config value is misconfigured during setup](https://github.com/cloudera-labs/hms-mirror/issues/155)
- [Enhancement for hms-mirror setup](https://github.com/cloudera-labs/hms-mirror/issues/154)
- [Remove automatic DB location as db_name.db in db location](https://github.com/cloudera-labs/hms-mirror/issues/153)
- [Add support for Hive table backup in DISTCP mode](https://github.com/cloudera-labs/hms-mirror/issues/152)

## 2.2.0.17.1

**What's New**

- [Extend / Split 'transfer-ownership' to apply to database and/or table](https://github.com/cloudera-labs/hms-mirror/issues/151)
- Improved Logging output
- [Support alternate db name for STORAGE_MIGRATION](https://github.com/cloudera-labs/hms-mirror/issues/149)
- CLI Option to control `data-movement-strategy`

**Bugs (Fixed)**
- [DB Prefix and DB Rename not working](https://github.com/cloudera-labs/hms-mirror/issues/150)
- [When Metastore isn't configured for ALIGNED/DISTCP, it's not writing MSCK](https://github.com/cloudera-labs/hms-mirror/issues/131)
- Fixed missing CR/LF is clean up scripts.

## 2.2.0.15.1

**What's New**

Feature that allows you to 'skip' modifying the database location during a storage migration.  This is useful if you're trying to archive tables in a database to another storage system, but want to leave the database location as is for new tables in the database. [For STORAGE_MIGRATION, add option that would skip any Database Location Adjustments](https://github.com/cloudera-labs/hms-mirror/issues/147)

![sm_skip_dblocs.png](sm_skip_dblocs.png)

## 2.2.0.15

**What's New**

- Add support for Oracle Metastore Direct Connection
- [SQL Strategy only uses MSCK for partition discovery for Shadow Table](https://github.com/cloudera-labs/hms-mirror/issues/145)

**Bugs (Fixed)**

- Output and Report Directory Consistency between CLI and Web UI.  See docs for more details.
- Postgres Metastore Direct Connection Fixes
- SQL Data Strategy Validation Blockers for Acid tables- 

## 2.2.0.12

**What's New**

- [For STORAGE_MIGRATION to Ozone, ensure valid Volume/Bucket names](https://github.com/cloudera-labs/hms-mirror/issues/142)

**Bugs (Fixed)**

- [Expose Stats in Web UI Reports](https://github.com/cloudera-labs/hms-mirror/issues/141)
- Fixed the ability to Encrypt/Decrypt password in the UI.
- [Execute run for Strategies without RIGHT cluster definition fail to write reports](https://github.com/cloudera-labs/hms-mirror/issues/144)
- [STORAGE_MIGRATION distcp efficiency option](https://github.com/cloudera-labs/hms-mirror/issues/143)

## 2.2.0.10

**What's New**

- [Hive 4 DB OWNER DDL syntax for ALTERing DB ONWER requires 'USER'](https://github.
  com/cloudera-labs/hms-mirror/issues/139)

This changed resulted in a simplification of how we determine what the cluster platform is. Previously we used two attributes (`legacyHive` and `hdpHive3`) to determine the platform.  This information would direct logic around translations and other features.  

Unfortunately, this isn't enough for us to determine all the scenarios we're encountering.  These attributes have been replaced with a new attribute call `platformType`. 

We will make automatic translations of legacy configurations to the new `platformType` attribute.  The translation will be pretty basic and result in either the platform type being defined as `HDP2` or `CDP_7.1`. If you have a more complex configuration, you'll need to adjust the `platformType` attribute manually.  Future persisted configurations will use the new `platformType` attribute and drop the `legacyHive` and `hdpHive3` attributes.

- [Add "Property Overrides" to Web Interface](https://github.com/cloudera-labs/hms-mirror/issues/111)

A feature that was late in making it into the Web UI is now here.

- [For Web UI Service, default to prefer IPV4](https://github.com/cloudera-labs/hms-mirror/issues/134)

To ensure the right IP stack is used when the Web UI starts up, we're forcing this JDK configuration with the Web UI.

- [Forcibly set Java Home via -Duser.home](https://github.com/cloudera-labs/hms-mirror/issues/136)

We had a few requests and issues with implementations were the target environment isn't always setup with normal user 'home' standards that we can rely on.  This change allows us to set the 'home' directory for the user running the application and ensure its translated correctly in hms-mirror for storing and reading configurations, reports, and logs.

If you are in an environment that doesn't follow user `$HOME` standards, you can set the `HOME` environment variable to a custom directory **BEFORE** starting `hms-mirror` to alter the default behavior.

### Cleanup SQL has been added to Web Reporting UI

We've added a 'Cleanup SQL' tab to the Web Reporting UI.  This will show you the SQL that was generated to clean up the source cluster after the migration.  This is useful to see what will be done before you execute the migration.

**Bugs (Fixed)**

- [DATABASE set OWNER ALTER statement is incorrect](https://github.com/cloudera-labs/hms-mirror/issues/135)
- [SQL ACID Migrations from HDPHive3 cluster Ordering](https://github.com/cloudera-labs/hms-mirror/issues/138)
- [DB Location for HDP3 migrations is flipped](https://github.com/cloudera-labs/hms-mirror/issues/140)

## 2.2.0.9

**Bugs (Fixed)**

- [Allow process path that doesn't require Metastore Direct Connection](https://github.com/cloudera-labs/hms-mirror/issues/128)
- [Fix Kerberos Connection Issues to HS2](https://github.com/cloudera-labs/hms-mirror/issues/129)
- [Job Progress getting stuck at the end while writing reports](https://github.com/cloudera-labs/hms-mirror/issues/130)

**Enhancements**

Increase build dependencies to CDP 7.1.9 SP1.
Rework Pass Key Management.
Additional details in Connection Validation.

## 2.2.0.8

**Bugs (Fixed)**

- [Not parsing abfs protocol locations correctly](https://github.com/cloudera-labs/hms-mirror/issues/124)
- [Database input duplication](https://github.com/cloudera-labs/hms-mirror/issues/125)

## 2.2.0.7

**Bugs (Fixed)**

- [Non Configs blocking WEB UI Create](https://github.com/cloudera-labs/hms-mirror/issues/122)
- [SCHEMA_ONLY with ALIGNED, DISTCP and Partitions is too granular on partition distcp](https://github.com/cloudera-labs/hms-mirror/issues/123)
- [Expose rid in web ui](https://github.com/cloudera-labs/hms-mirror/issues/121)

## 2.2.0.5

**Bugs (Fixed)**

- [CLI Setup Issues](https://github.com/cloudera-labs/hms-mirror/issues/120)
- [Stale Config after several runs](https://github.com/cloudera-labs/hms-mirror/issues/118)
- [SQL Strategy Transfer Fixes](https://github.com/cloudera-labs/hms-mirror/issues/117)
- [Partition Reductions for Distcp](https://github.com/cloudera-labs/hms-mirror/issues/116)

## 2.2.0.4

**Bugs (Fixed)**

- [Report Output Directory Issue](https://github.com/cloudera-labs/hms-mirror/issues/115)
- [Filters Optimizations and Fixes](https://github.com/cloudera-labs/hms-mirror/issues/116)

## 2.2.0.2

This is a big release for `hms-mirror`. We've added a Web interface to `hms-mirror` that makes it easier to configure
and run varies scenarios.

Along with the Web interface, we've made some significant adjustments to the `hms-mirror` engine which is much more
complete than the previous release. The engine now supports a wider range of strategies and has a more robust
configuration system.

We do our best to guide you through configurations that make sense, help you build plans and manage complex scenarios.

### Automatic Configuration Adjustments

To ensure that configuration settings are properly set, the application will automatically adjust the configuration
settings to match a valid scenario. These changes will be recorded in the 'run status config messages' section and can
be seen on reports or the web interface.

Changes are mostly related to the acceptable strategy configurations. See [Location Alignment](Location-Alignment.md)
for more details.

### Property Overrides

Not yet available in Web UI. Coming soon [issue 111](https://github.com/cloudera-labs/hms-mirror/issues/111).

This feature, introduced in the CLI, allows you to add/override Hive properties on the LEFT, RIGHT, or BOTH clusters for custom control of running Hive jobs. Most commonly used with SQL migration strategies.

### Evaluate Partition Locations and Reset to Default Location

These properties are no longer valid. An added property called 'translationType' is used to determine this
functionality.

Before the `epl|evaluate-partition-locations` would gather partition location information from the Metastore Direct
connection to ensure they were aligned. We've adjusted/simplified the concept with `translationType`, which defined
either `RELATIVE` or `ALIGNED` strategy types.

See [Location Alignment](Location-Alignment.md) for more details.

### Concurrency

In previous releases using the CLI, concurrency could be set through the configuration files `transfer:concurrency`
setting. The default was 4 if no setting was provided. This setting in the control file is NO LONGER supported and will
be ignored. The new default concurrency setting is `10` and can be overridden only during the application startup.

See [Concurrency](Concurrency.md) for more details.

### Global Location Maps

Previous releases had a fairly basic implementation of 'Global Location Maps'. These could be supplied through the
cli option `-glm`, which is still supported, but limited in functionality. The improved implementation work from the
concept of building 'Warehouse Plans' which are then used to build the 'Global Location Maps'.

See [Warehouse Plans](Database-Warehouse-Plans.md) for more details.

The `-glm` option can take an addition element to identify the mapping for a particular table type. As a result, any
configuration files save with this setting will not be loaded and will need to be updated.

While the `-glm` option will still honor the old format of `source_dir=target_dir`, the new format
is `source_dir:<table_type>:target_dir`. The `table_type` is a new addition to the configuration and is required for the
new implementation. When omitted, the mapping will be created for both EXTERNAL and MANAGED tables.

`<table_type>` can be one of: `EXTERNAL_TABLE` or `MANAGED_TABLE`.

`-glm /tpsds_base_dir=EXTERNAL_TABLE:/alt/ext/location`

**Old Format**

```yaml
globalLocationMap:
  /tpcds_base_dir: "/alt/ext/location"
  /tpcds_base_dir2/web: "/alt/ext/location"
```

**New Format**

```yaml
userGlobalLocationMap:
  /tpcds_base_dir:
    EXTERNAL_TABLE: "/alt/ext/location"
  /tpcds_base_dir2/web:
    EXTERNAL_TABLE: "/alt/ext/location"
```

### JDK 11 Support

The application now supports JDK 11, as well as JDK 8.

### Kerberos Support and Platform Libraries

We are still working to replicate the options available in previous release with regard to Kerberos connections.
Currently, `hms-mirror` can only support a single Kerberos connection. This is the same as it was
previously.  `hms-mirror` packaging includes the core Hadoop classes required for Kerberos connections pulled from the
latest CDP release.

In the past, we 'could' support kerberos connections to lower versions of Hadoop clusters (HDP and CDH) by
running `hms-mirror` on a cluster with those hadoop libraries installed and specifying `--hadoop-classpath` on the
commandline. This is no longer supported, as the packaging required to support the Web and REST interfaces is now
different.

We are investigating the possibility of supporting kerberos connections to lower clusters in the future.

### Metastore Direct Access

In later 1.6 releases we introduced a 'Metastore Direct' connection type when defining a LEFT(source) cluster. To help
build a more complete picture of locations in the metadata, we found it necessary to gather detailed location
information for each partition of the datasets being inspected. Because Hive was so configurable regarding location
preferences and the ability to set locations at the partition level, we needed to ensure that the locations were
aligned. The only sure way to get this complete picture was to connect directly to the Metastore backend database. We
currently support 'MYSQL' and 'POSTGRES' metastore backends.  'Oracle' coming soon.

