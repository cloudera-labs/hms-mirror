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


## Issue 2

Caused by: java.lang.RuntimeException: org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAccessControlException:Permission denied: user [dstreev] does not have [ALL] privilege on [hdfs://HDP50/apps/hive/warehouse/tpcds_bin_partitioned_orc_10.db/web_site]

Checked permissions of 'dstreev': Found that the 'dstreev' user was NOT the owner of the files in these directories. The user running the process needs to be in 'dfs.permissions.superusergroup' for the lower clusters 'hdfs' service.  Ambari 2.6 has issues setting this property: https://jira.cloudera.com/browse/EAR-7805

Follow workaround above or add use the 'hdfs' group. Or use Ranger to allow all access. On my cluster, with no Ranger, I had to use '/var/lib/ambari-server/resources/scripts/configs.py' to set it manually for Ambari.

`sudo ./configs.py --host=k01.streever.local --port=8080 -u admin -p admin -n hdp50 -c hdfs-site -a set -k dfs.permissions.superusergroup -v hdfs_admin`


## Tip 1 (not validated)

Increase the 'hive_on_tez' `hive.msck.repair.batch.size` to improve partition discovery times.

HMS:
`hive.exec.input.listing.max.threads`
`hive.load.dynamic.partitions.thread`
`hive.metastore.fshandler.threads` (seemed to be the only one the cm noticed on restart)

## Tip 2

It seems that when there are ".hive-staging..." files in a table directory that is having partitions build for either by MSCK or ALTER SET LOCATION, it REALLY slows down the process.