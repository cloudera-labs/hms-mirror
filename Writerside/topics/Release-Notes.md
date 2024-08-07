# Release Notes

## v.2.x Changes

This is a big release for `hms-mirror`.  We've added a Web interface to `hms-mirror` that makes it easier to configure and run varies scenarios.

Along with the Web interface, we've made some significant adjustments to the `hms-mirror` engine which is much more complete than the previous release.  The engine now supports a wider range of strategies and has a more robust configuration system.

We do our best to guide you through configurations that make sense, help you build plans and manage complex scenarios.

## Automatic Configuration Adjustments

To ensure that configuration settings are properly set, the application will automatically adjust the configuration settings to match a valid scenario. These changes will be recorded in the 'run status config messages' section and can be seen on reports or the web interface.

Changes are mostly related to the acceptable strategy configurations.  See [Location Alignment](Location-Alignment.md) for more details.

## Evaluate Partition Locations and Reset to Default Location

These properties are no longer valid.  An added property called 'translationType' is used to determine this functionality. 

Before the `epl|evaluate-partition-locations` would gather partition location information from the Metastore Direct connection to ensure they were aligned.  We've adjusted/simplified the concept with `translationType`, which defined either `RELATIVE` or `ALIGNED` strategy types.

See [Location Alignment](Location-Alignment.md) for more details.

## Concurrency

In previous releases using the CLI, concurrency could be set through the configuration files `transfer:concurrency` setting.  The default was 4 if no setting was provided.  This setting in the control file is NO LONGER supported and will be ignored.  The new default concurrency setting is `10` and can be overridden only during the application startup.

See [Concurrency](Concurrency.md) for more details.

## Global Location Maps

Previous releases had a fairly basic implementation of 'Global Location Maps'.  These could be supplied through the 
cli option `-glm`, which is still supported, but limited in functionality. The improved implementation work from the 
concept of building 'Warehouse Plans' which are then used to build the 'Global Location Maps'. 

See [Warehouse Plans]() for more details.

The `-glm` option can take an addition element to identify the mapping for a particular table type. As a result, any configuration files save with this setting will not be loaded and will need to be updated.

While the `-glm` option will still honor the old format of `source_dir=target_dir`, the new format is `source_dir:<table_type>:target_dir`.  The `table_type` is a new addition to the configuration and is required for the new implementation.  When omitted, the mapping will be created for both EXTERNAL and MANAGED tables.

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

## JDK 11 Support

The application now supports JDK 11, as well as JDK 8.

## Kerberos Support and Platform Libraries

We are still working to replicate the options available in previous release with regard to Kerberos connections.  Currently, `hms-mirror` can only support a single Kerberos connection.  This is the same as it was previously.  `hms-mirror` packaging includes the core Hadoop classes required for Kerberos connections pulled from the latest CDP release.

In the past, we 'could' support kerberos connections to lower versions of Hadoop clusters (HDP and CDH) by running `hms-mirror` on a cluster with those hadoop libraries installed and specifying `--hadoop-classpath` on the commandline. This is no longer supported, as the packaging required to support the Web and REST interfaces is now different.

We are investigating the possibility of supporting kerberos connections to lower clusters in the future.

## Metastore Direct Access

In later 1.6 releases we introduced a 'Metastore Direct' connection type when defining a LEFT(source) cluster. To help build a more complete picture of locations in the metadata, we found it necessary to gather detailed location information for each partition of the datasets being inspected.  Because Hive was so configurable regarding location preferences and the ability to set locations at the partition level, we needed to ensure that the locations were aligned.  The only sure way to get this complete picture was to connect directly to the Metastore backend database.  We currently support 'MYSQL' and 'POSTGRES' metastore backends.  'Oracle' coming soon.

