# Pre-Requisites

## Hive/TEZ Properties Whitelist Requirements

HiveServer2 has restrictions on what properties can be set by the user in a session.  To ensure that `hms-mirror` will be able to set the properties it needs, add [`hive.security.authorization.sqlstd.confwhitelist.append`](https://cwiki.apache.org/confluence/display/Hive/Configuration+Properties#ConfigurationProperties-hive.security.authorization.sqlstd.confwhitelist.append) property in the HiveServer2 Advanced Configuration Snippet (Safety Valve) for `hive-site.xml` with at least the following value(s) so `hms-mirror` can set the properties it needs:

```
tez\.grouping\..*
```

## Backups

DO NOT SKIP THIS!!!

The <tooltip term="hms-mirror">`hms-mirror`</tooltip> process DOES 'DROP' tables when asked.  If those tables *manage* data like a *legacy* managed, *ACID*, or *external.table.purge=true* scenario, we do our best NOT to DROP those and ERROR out.  But, protect yourself and make backups of the areas you'll be working in.

### HDFS Snapshots

Use HDFS Snapshots to make a quick backup of directories you'll be working on.  Do this, especially in the *LEFT* cluster.  We only drop tables, so a snapshot of the database directory is good.  BUT, if you are manually doing any `DROP DATABASE <x> CASCADE` operations, that will delete the snapshotted directory (and the snapshot).  In this case, create the _snapshot_ one level above the database directory.

### Metastore Backups

Take a DB backup of your metastore and keep it in a safe place before starting.

## Shared Authentication

The clusters must share a common authentication model to support cross-cluster HDFS access when HDFS is the underlying storage layer for the datasets.  This means that a **kerberos** ticket used in the RIGHT cluster must be valid for the LEFT cluster.

For cloud storage, the two clusters must have rights to the target storage bucket.

If you can [`distcp` between the clusters](Linking-Cluster-Storage-Layers.md), you have the basic connectivity required to start working with `hms-mirror`.
