# Optimizations

Moving metadata and data between two clusters is a pretty straight forward process, but depends entirely on the proper configurations in each cluster.  Listed here are a few tips on some important configurations.

### Make Backups before running `hms-mirror`

Take snapshots of areas you'll touch:
- The HMS database on the LOWER and UPPER clusters
- A snapshot of the HDFS directories on BOTH the LOWER and UPPER clusters that will be used/touched.

### Isolate Migration Activities

The migration of schemas can put a heavy load on HS2 and the HMS server it's using.  That impact can manifest itself as 'pauses' for other clients trying to run queries.  Long schema/discovery operations have a 'blocking' tendency in HS2.

To prevent normal user operational impact, I suggest establishing an isolated HMS and HS2 environment for the migration process.

![Isolate Migration Service Endpoints](./images/isolation.png)

### `ranger.plugin.hive.urlauth.filesystem.schemes=file`

Set this in the Hive Server 2(hive_on_tez) Ranger Plugin Safety Value, via Cloudera Manager.

![Safety Value](./images/hs2_ranger_schemas.png)

Add this to the HS2 instance on the UPPER cluster, when Ranger is used for Auth.
This skips the check done against every directory at the table location (for CREATE or ALTER LOCATION).  Allowing the process of CREATE/ALTER to run much faster.

The default (true) behavior works well for interactive use case, but bulk operations like this can take a really long time if this validation needs to happen for every new partition during creation or discovery.

I recommend turning this back after the migration is complete.  This setting exposes permissions issues at the time of CREATE/ALTER.  So by skipping this, future access issues may arise if the permissions aren't aligned, which isn't a Ranger/Hive issue, it's a permissions issue.

### Turn ON HMS partition discovery

In CDP 7.1.4 and below, the house keeping threads in HMS used to discover partitions is NOT running.  Add `metastore.housekeeping.threads.on=true` to the HMS Safety Value to activate the partition discovery thread.  Once this has been set, the following parameters can be used to modify the default behavior.

```
metastore.partition.management.task.frequency
hive.exec.input.listing.max.threads
hive.load.dynamic.partitions.thread
hive.metastore.fshandler.threads 
```

The default batch size for partition discovery via `msck` is 3000.  Adjustments to this can be made via the `hive.msck.repair.batch.size` property in HS2.
