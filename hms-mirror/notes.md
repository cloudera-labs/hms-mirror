# Notes

The application is built against CDP 7.1 binaries.

## Issue 1

Connecting to an HDP cluster running 2.6.5 with Binary protocol and Kerberos triggers an incompatibility issue:
`Unrecognized Hadoop major version number: 3.1.1.7.1....`

### Solution

The application is built with CDP libraries (excluding the Hive Standalone Driver).  When `kerberos` is the `auth` protocol to connect to **Hive 1**, it will get the application libs which will NOT be compatible with the older cluster.

Kerberos connections are only supported to the CDP cluster.  

When connecting via `kerberos`, you will need to include the `--hadoop-classpath` when launching the application.     

## Features

- Add PARTITION handling
- Add Stages
- Add Check for existing table on upper cluster.
    - option for override def in stage 1.
    
    
When the "EXTERNAL" tables are added to the UPPER cluster, they're added with "discover.partitions"="true".  But they
don't appear to get fixed in CDP.  https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DDL#LanguageManualDDL-DiscoverPartitions

This appears to be a feature in Hive 4.0.  Need to see about backport to CDP.
When Hive Metastore Service (HMS) is started in remote service mode, a background thread (PartitionManagementTask) gets scheduled periodically every 300s (configurable via metastore.partition.management.task.frequency config) that looks for tables with "discover.partitions" table property set to true and performs msck repair in sync mode. If the table is a transactional table, then Exclusive Lock is obtained for that table before performing msck repair. With this table property, "MSCK REPAIR TABLE table_name SYNC PARTITIONS" is no longer required to be run manually. 