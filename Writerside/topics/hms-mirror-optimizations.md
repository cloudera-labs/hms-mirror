# Optimizations

Moving metadata and data between two clusters is a pretty straightforward process but depends entirely on the proper configurations in each cluster.  Listed here are a few tips on some crucial configurations.

HMS-Mirror only moves data with the [SQL](hms-mirror-sql.md) and [EXPORT_IMPORT](hms-mirror-export-import.md) data 
strategies.  All other strategies either use the data as-is ([LINKED](hms-mirror-linked.md) or [COMMON](hms-mirror-common.md)) or depend on the data being moved by something like `distcp`.

## Controlling the YARN Queue that runs the SQL queries from `hms-mirror`

Use the jdbc url defined in `default.yaml` to set a queue.

`jdbc:hive2://host:10000/.....;...?tez.queue.name=batch`

The commandline properties `-po`, `-pol`, and `-por` can be used to override the queue name as well. For example: `-pol tez.queue.name=batch` will set the queue for the "LEFT" cluster while `-por tez.queue.name=migration` will set the queue for the "RIGHT" cluster.

## Make Backups before running `hms-mirror`

Take snapshots of areas you'll touch:
- The HMS database on the LEFT and RIGHT clusters
- A snapshot of the HDFS directories on BOTH the LEFT and RIGHT clusters will be used/touched.
  > NOTE: If you are testing and "DROPPING" dbs, Snapshots of those data directories could protect you from accidental deletions if you don't manage purge options correctly.  Don't skip this...
  > A snapshot of the db directory on HDFS will prevent `DROP DATABASE x CASCADE` from removing the DB directory (observed in CDP 7.1.4+ as tested, check your version) and all sub-directories even though tables were NOT configured with `purge` options.

## Isolate Migration Activities

The migration of schemas can put a heavy load on HS2 and the HMS server it's using.  That impact can manifest itself as 'pauses' for other clients trying to run queries. Extended schema/discovery operations have a 'blocking' tendency in HS2.

To prevent average user operational impact, I suggest establishing an isolated HMS and HS2 environment for the migration process.

![Isolate Migration Service Endpoints](images/isolation.png)

## Speed up CREATE/ALTER Table Statements - with existing data

Set `ranger.plugin.hive.urlauth.filesystem.schemes=file` in the Hive Server 2(hive_on_tez) Ranger Plugin Safety Value, via Cloudera Manager.

![Safety Value](images/hs2_ranger_schemas.png)

Add this to the HS2 instance on the RIGHT cluster when Ranger is used for Auth.
This skips the check done against every directory at the table location (for CREATE or ALTER LOCATION). It is allowing the process of CREATE/ALTER to run much faster.

The default (true) behavior works well for the interactive use case. Still, bulk operations like this can take a long time if this validation needs to happen for every new partition during creation or discovery.

I recommend turning this back after the migration is complete.  This setting exposes permissions issues at the time of CREATE/ALTER.  So by skipping this, future access issues may arise if the permissions aren't aligned, which isn't a Ranger/Hive issue, it's a permissions issue.

## Turn ON HMS partition discovery

In CDP 7.1.4 and below, the housekeeping threads in HMS used to discover partitions are NOT running.  Add `metastore.housekeeping.threads.on=true` to the HMS Safety Value to activate the partition discovery thread.  Once this has been set, the following parameters can be used to modify the default behavior.

```
hive.metastore.partition.management.task.frequency
hive.exec.input.listing.max.threads
hive.load.dynamic.partitions.thread
hive.metastore.fshandler.threads 
```

### Source Reference

```
    METASTORE_HOUSEKEEPING_LEADER_HOSTNAME("metastore.housekeeping.leader.hostname",
            "hive.metastore.housekeeping.leader.hostname", "",
"If multiple Thrift metastore services are running, the hostname of Thrift metastore " +
        "service to run housekeeping tasks at. By default, this value is empty, which " +
        "means that the current metastore will run the housekeeping tasks. If configuration" +
        "metastore.thrift.bind.host is set on the intended leader metastore, this value should " +
        "match that configuration. Otherwise it should be same as the hostname returned by " +
        "InetAddress#getLocalHost#getHostName(). Given the uncertainty in the later " +
        "it is desirable to configure metastore.thrift.bind.host on the intended leader HMS."),
    METASTORE_HOUSEKEEPING_THREADS_ON("metastore.housekeeping.threads.on",
        "hive.metastore.housekeeping.threads.on", false,
        "Whether to run the tasks under metastore.task.threads.remote on this metastore instance or not.\n" +
            "Set this to true on one instance of the Thrift metastore service as part of turning\n" +
            "on Hive transactions. For a complete list of parameters required for turning on\n" +
            "transactions, see hive.txn.manager."),
```

The default batch size for partition discovery via `msck` is 3000.  Adjustments to this can be made via the `hive.msck.repair.batch.size` property in HS2.
