# Release Notes

## Known Issues

The latest set of known issues can be
found [here](https://github.com/cloudera-labs/hms-mirror/issues?q=is%3Aissue+is%3Aopen+label%3Abug)

## Enhancement Requests

The latest set of enhancement requests can be
found [here](https://github.com/cloudera-labs/hms-mirror/issues?q=is%3Aissue+is%3Aopen+label%3Aenhancement). 

If there is
something you'd like to see, add a new issue [here](https://github.com/cloudera-labs/hms-mirror/issues)

## 2.2.0.10

**What's New**

### [Hive 4 DB OWNER DDL syntax for ALTERing DB ONWER requires 'USER'](https://github.com/cloudera-labs/hms-mirror/issues/139)

This changed resulted in a simplification of how we determine what the cluster platform is. Previously we used two attributes (`legacyHive` and `hdpHive3`) to determine the platform.  This information would direct logic around translations and other features.  

Unfortunately, this isn't enough for us to determine all the scenarios we're encountering.  These attributes have been replaced with a new attribute call `platformType`. A list of the platform types can be found [here]().

We will make automatic translations of legacy configurations to the new `platformType` attribute.  The translation will be pretty basic and result in either the platform type being defined as `HDP2` or `CDP_7.1`. If you have a more complex configuration, you'll need to adjust the `platformType` attribute manually.  Future persisted configurations will use the new `platformType` attribute and drop the `legacyHive` and `hdpHive3` attributes.

### [Add "Property Overrides" to Web Interface](https://github.com/cloudera-labs/hms-mirror/issues/111)

A feature that was late in making it into the Web UI is now here.

### [For Web UI Service, default to prefer IPV4](https://github.com/cloudera-labs/hms-mirror/issues/134)

To ensure the right IP stack is used when the Web UI starts up, we're forcing this JDK configuration with the Web UI.

### [Forcibly set Java Home via -Duser.home](https://github.com/cloudera-labs/hms-mirror/issues/136)

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

See [Warehouse Plans]() for more details.

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

