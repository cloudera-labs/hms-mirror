## Backups

DO NOT SKIP THIS!!!

The `hms-mirror` process DOES 'DROP' tables when asked to.  If those tables *manage* data like a *legacy* managed, *ACID*, or *external.table.purge=true* scenario, we do our best to NOT DROP those and ERROR out.  But, protect yourself and make backups of the areas you'll be working in.

### HDFS Snapshots

Use HDFS Snapshots to make a quick backup of directories you'll be working on.  Do this especially in the *LEFT* cluster.  We only drop tables, so a snapshot of the database directory is good.  BUT, if you are manually doing any `DROP DATABASE <x> CASCADE` operations, that will delete the snapshotted directory (and the snapshot).  In this case, create the _snapshot_ one level above the database directory.

### Metastore Backups

Take a DB backup of your metastore and keep it in a safe place before starting.