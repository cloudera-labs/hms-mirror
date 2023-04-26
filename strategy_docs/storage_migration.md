## STORAGE_MIGRATION

The "STORAGE_MIGRATION" data strategy is intended for migrating data stored on one Filesystem to another while maintaining the same metadata presentation to the user.

The primary use case design is to migrate **Hive** data from _HDFS_ to _Ozone_.  Other possibilities for this strategy include migrating **Hive** data from _HDFS_ to _Cloud Storage_.

This strategy applies **ONLY** to the LEFT cluster defined in the _configuration_.  If the target cluster is defined as the RIGHT cluster, use the `-f|--flip` option at the command line to promote the RIGHT cluster to the LEFT when you launch.

There are subtle adjustments made to the migrated tables based on the `clusters:LEFT:legacyHive` setting.  Be sure to set this correctly for the target cluster.

This strategy will use SQL to migrate data between the two filesystems.

### Building the Data Path (*table* **LOCATION**)

The table *LOCATION* element is removed for ACID tables and downgraded ACID tables.  The resulting datasets will go to the **default** location defined for the database.  That location is a combination of the new namespace, the warehouse directory (table type-specific), the name of the database, and the table's name.

All other table locations will replace the original namespace defined for the cluster `clusters:LEFT:hcfsNamespace` with the new namespace defined by the `-smn` command-line option.  The default behavior/intent is to keep the same relative directory structure for **external** tables to minimize the impact on external engines/integrators by maintaining as much of the path as possible.

#### Example Path Conversions

| Table Type | Original Table<br/>Location | DB Name | `-smn` | `-wd` or `-ewd` | New Location | Note |
|:---|:---|:---|:---|:---|:---|:---|
| EXT | hdfs://prodcluster/warehouse/tablespace/external/hive/click.db/web_events | web_events | ofs://OHOME90 | `-ewd` = /warehouse/external | ofs://OHOME90/warehouse/tablespace/external/hive/click.db/web_events | The tables relative location is maintained here.  The `-ewd` setting only affects NEW tables that are create WITHOUT a LOCATION element. |
| ACID | hdfs://prodcluster/warehouse/tablespace/managed/hive/click.db/web_events | web_events | ofs://OHOME90 | `-wd` = /warehouse/managed | ofs://OHOME90/warehouse/managed/click.db/web_events | The tables relative location is overriden since it is a managed table.  Managed tables pick up the location from the 'database' definition or the 'metastore'. Here, we defined the `-wd` element, which overrides the database *MANAGEDLOCATION* property. |
| ACID (downgrading) | hdfs://prodcluster/warehouse/tablespace/managed/hive/click.db/web_events | web_events | ofs://OHOME90 | `-ewd` = /warehouse/external | ofs://OHOME90/warehouse/external/click.db/web_events | The tables relative location is overriden since it *WAS* a managed table and is being *downgraded*.  In this case, the relative location isn't valid anymore.  We'll leave it to the system default location to store this tables data, which we've overriden with the `-ewd` setting to sets the databases *LOCATION* property, used to control the default location of NEW external tables.|

Consider the combination of the settings for `-smn`, and `-wd` or `-ewd` carefully.   For protocols like Ozone, the path AFTER the namespace defines the Ozone Volume.  If you have different encryption needs between *MANAGED* and *EXTERNAL* datasets, the second path element, which establishes the _bucket_, could be an essential distinction that enables this requirement.

### Cleanup

After you've run `hms-mirror` for STORAGE_MIGRATION, by either `-e` or _manually_, you'll be left with some artifact tables.  These are the original tables that were renamed to make room for the new table definitions at the new locations.  Each database report will contain a file called `<db_name>_LEFT_CleanUp_execute.sql`.  This `sql` file contains the `DROP TABLE` commands needed to cleanup these artifacts.  Check your work before running this!!!

### Required Command-Line Elements

#### `-smn|--storage-migration-namespace`

Table data will be migrated to this new namespace.  The value includes the *protocol*, which requires the platform to have the appropriate namespace libraries available, namespace, and optional root path information.  Here are a few examples:

```
ofs://OHOME90 -- An Ozone Namespace
ofs://OHOME90/warehouse -- An Ozone Namespace and Volume
s3a://my_target_bu_bucket -- An S3 Bucket
```

#### Warehouse Directories

Under some conditions, migrating DDL will NOT include a 'LOCATION' element in the table definition used to build the new dataset.  The omission happens for ACID table migrations and 'downgraded' ACID table migrations.

The two required elements below will be used to **ALTER** the **Database** definition to include the new namespace `-smn` and these locations.

`-wd|--warehouse-directory`
`-ewd|--external-warehouse-directory`

The definitions will, by default, override the system _warehouse_ locations defined in the _Hive Metastore_.  New table DDL submitted without a *LOCATION* will have their data directed to these locations based on the table type.


### Optional Elements

#### `-rdl|--reset-to-default-location`

Migrating Storage is an opportunity to reorganize your datasets on the Filesystem.  As a part of the storage migration, you may desire to establish better governance around your data.  And resetting where that data goes by using the default location may help with the effort.

ALL tables will be migrated **WITHOUT** a *LOCATION* element when specifying `-rdl`.  A combination of `-smn` and `-wd` or `-ewd` will guide their final locations.

It's important to note that this option **contradicts** the intent to minimize data impact by maintaining *relative* locations.

#### `-ma|--migrate-acid` or `-mao|--migrate-acid-only`

Select to migrate ACID tables.  Each option accepts an optional argument, `ArtificialBucketThreshold`, that defines a threshold on the number of buckets that would be considered *Artificial*.   When/If an ACID table has a bucket count equal to or less than this value, the bucket definition will be **removed** in the migrated table.  The system default is `2`, so any table defined with `2` or less buckets will have the bucket definition removed.  If you wish to keep ALL bucket definitions, set the value to `-1`.

#### `-da|--downgrade-acid`

Downgrading ACID tables will convert them into **EXTERNAL/PURGE** tables, and they will no longer be ACID capable. 

#### `-dc|--distcp`

When `-dc|--distcp` is requested, `hms-mirror` will build a `distcp` plan in the report output directory with details to support the requested transfers.  The `distcp` commands are normally run from the LEFT cluster and will require visibility, configuration, and permissions to the target location.  So if the target location is storage in the Cloud, ensure that the LEFT cluster has been configured with the appropriate storage libraries and has the required 'keys' and/or 'shared' secrets required to connect.  If you can not access the target storage area with `hdfs dfs` commands on the LEFT cluster, then the `distcp` command will fail too.

Since `distcp` doesn't convert any files, it just migrates them as is, the option can NOT be used to migrate ACID tables.

The option also requires the `distcp` actions to take place BEFORE the target locations are created on the target Filesystem.  If the schemas are converted before the data is migrated, the tables will be empty.  Hence, the actions need to be performed manually and the `-e|--execute` option isn't supported.

*Action Order Option #1*

- Run `hms-mirror` with `STORAGE_MIGRATION` and `distcp`
- Using the `distcp` workplan, run the `distcp` process to migrate the data.
- Run the `LEFT` execute SQL to adjust database and table location details.

Under normal circumstances this process will work fine.  But, if you're moving tables with a massive number of files and partitions, you may run into an issue running the table `CREATE` statements when the data exist (post `distcp`).  Hive's normal behaviour is to 'recursively' validate file permissions for each and every directory/file.  When the counts are high, a `CREATE` operation can take a very long time.  Consider option #2

*Action Order Option #2*

- Run `hms-mirror` with `STORAGE_MIGRATION` and `distcp`
- Run the `LEFT` execute SQL to adjust database and table location details.
- Using the `distcp` workplan, run the `distcp` process to migrate the data.
- If the tables aren't set to `auto-discover` partitions, you will need to run `msck repair table <table_name> SYNC PARTITIONS` after the `distcp` job.

The obvious downside to this is that the tables will be empty, until the `distcp` job is complete.  But it avoids the ACL checks in option #1 and the `CREATE` statements will run quickly.

### Example Reports

[Literal Translation Dry-run](../sample_reports/storage_migration/literal)

[Literal Translation Dry-run w/ `-dc`](../sample_reports/storage_migration/literal-dc)

[Literal Translation Exec](../sample_reports/storage_migration/literal-exec)

[Reset-Default-Location Dry-run](../sample_reports/storage_migration/rdl)

[Reset-Default-Location Exec](../sample_reports/storage_migration/rdl-exec)