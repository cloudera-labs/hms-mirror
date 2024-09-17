# Hive Conversions

Hive has gone through a lot of changes as it's evolved over the last several years.
Especially between Hive 1/2 and Hive 3.  The default syntax used to 'create' tables
hasn't changed, but the resulting table structure may have.

Understand those changes and what hive flags are available to help influence that structure
aren't very clear to even the most seasoned Hive user.

`hms-mirror` uses settings in the 'cluster' configuration to influence **how** tables are translated
during the migration.

<tabs>
<tab id="webui" title="Web Interface">
Set the properties in the appropriate cluster configuration(s) for your strategy.

![cluster_tabs.png](cluster_tabs.png)

Set the flags that match you cluster's Hive version.

![hive_version_flags.png](hive_version_flags.png)

</tab>
<tab id="cli" title="CLI">
Modify the `hms-mirror` configuration to include the following settings:

``` yaml
clusters:
  LEFT|RIGHT:
    platformType: HDP2|HDP3|CHD5|CDH6|CDP7.1|CDP7_2|...
```
</tab>
</tabs>

If you were upgrading in-place a Hive 1/2 cluster to Hive 3, the Hive upgrade process would convert 'legacy' managed 
tables to 'EXTERNAL' tables and add a 'PURGE' flag in the table properties so that tables behavior remains 
consistent with the original Hive 1/2 behavior while also being a compatible Hive 3 table.

Managed non-ACID tables are NOT a valid state in Hive 3.  

With the properties set in the configuration for the cluster (above), `hms-mirror` will make these conversions for you, in the same way that the Hive upgrade process would.

## Hive 1/2 Table Types History

Tables in Hive 1/2 are typically 'external' tables.  This means that the data is managed independently
of the Hive Metastore.  The table definition in the Hive Metastore points to the location of the data but does NOT
manage the data.  In this case, if you drop the table (or a partition), the data remains on the filesystem. 
These are created with the `CREATE EXTERNAL TABLE` command.

Another table type in Hive 1/2 is the 'managed' table.  This is where Hive manages the data.  If you drop the table 
(or partition), Hive will clean up the data on the filesystem.  These are created with the `CREATE TABLE` command.

There is also a variant of the 'managed' table that is ACID compliant.  These are created with the `CREATE TABLE` 
command with table properties that enable ACID characteristics `transactional=true`.  These tables have special 
characteristics and are not friendly to the 'schema on read' paradigm, since these tables are 'schema on write' and 
embed special columns in the data files.

Starting with Hive 3, the default behavior of the `CREATE TABLE` command is to create a 'managed' ACID table.  The 
additional table properties for a 'transactional' table are no longer required to create an ACID table.  This is a
significant change from Hive 1/2.

There is a web application that allows you to experiment with various
commands and settings, showing the resulting table structures.

Regardless, use the following web application to help you quickly through the trial and error
phase of understanding these new structural and session settings.

[Hive Create Path](https://dstreev.github.io/hive/create_path.html)