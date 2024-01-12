# Troubleshooting

## Application doesn't seem to be making progress

All the counters for table processing aren't moving (review the hms-mirror.log) or (1.6.1.0+) the on screen logging of what tables are being added and metadata collected for has stopped.

**Solution**

The application creates a pool of connection to the HiveServer2 instances on the LEFT and RIGHT to be used for processing.  When the HiveServer2 doesn't support or doesn't have available the number of connections being requested from `hms-mirror`, the application will 'wait' forever on getting the connections requested.

Stop the application and lower the concurrency value to a value that can be supported.

```yaml
transfer:
  concurrency: 4
```

Or, you could modify the HiveServer2 instance to handle the number of connections being requested.

## Application won't start `NoClassDefFoundError`

Error
```
Exception in thread "main" java.lang.NoClassDefFoundError:
java/sql/Driver at java.base/java.lang.ClassLoader.defineClass1
```

`hms-mirror` uses a classloader to separate the various jdbc classes (and versions) used to manage migrations between two different clusters. The application also has a requirement to run on older platforms, so the most common denominator is Java 8.  Our method of loading and separating these libraries doesn't work in Java 9+.

**Solution**


Please use Java 8 to run `hms-mirror`.

## CDP Hive Standalone Driver for CDP 7.1.8 CHF x (Cummulative Hot Fix) won't connect

If you are attempting to connect to a CDP 7.1.8 clusters Hive Server 2 with the CDP Hive Standalone Driver identified in the clusters `jarFile` property, you may not be able to connect. A security item addressed in these drivers changed the required classes.

If you see:

```
java.lang.RuntimeException: java.lang.RuntimeException: java.lang.NoClassDefFoundError: org/apache/log4j/Level
```

You will need to include additional jars in the `jarFile` property.  The following jars are required:

```
log4j-1.2-api-2.18.0.ja
log4j-api-2.18.0.jar
log4j-core-2.18.0.jar
```

The feature enhancement that allows multiple jars to be specified in the `jarFile` property is available in `hms-mirror` 1.6.0.0 and later. See [Issue #47](https://github.com/cloudera-labs/hms-mirror/issues/67)

**Solution**

Using `hms-mirror` v1.6.0.0 or later, specify the additional jars in the `jarFile` property. For example:
`jarFile: "<absolute_path_to>/hive-jdbc-3.1.3000.7.1.8.28-1-standalone.jar:<absolute_path_to>/log4j-1.2-api-2.18.0.jar:<absolute_path_to>/log4j-api-2.18.0.jar:<absolute_path_to>/log4j-core-2.18.0.jar"`

These jar files can be found on the CDP edge node in `/opt/cloudera/parcels/CDH/jars/`.

Ensure that the standalone driver is list 'FIRST' in the `jarFile` property.

## Failed AVRO Table Creation

```
Error while compiling statement: FAILED: Execution Error, return code 40000 from org.apache.hadoop.hive.ql.ddl.DDLTask. java.lang.RuntimeException: MetaException(message:org.apache.hadoop.hive.serde2.SerDeException Encountered AvroSerdeException determining schema. Returning signal schema to indicate problem: Unable to read schema from given path: /user/dstreev/test.avsc)
```

**Solution**

Validate that the 'schema' file has been copied over to the new cluster.  If that has been done, check the permissions.  In a non-impersonation environment (doas=false), the `hive` user must have access to the file.

## Table processing completed with `ERROR.`

We make various checks as we perform the migrations, and when those checks don't pass, the result is an error.

**Solution**

In [tips](hms-mirror-tips.md) we suggest running with `dry-run` first (default).  This will catch the potential issues first, without taking a whole lot of time.  Use this to remediate issues before executing.

If the scenario that causes the `ERROR` is known, a remediation summary will be in the output report under **Issues** for that table.  Follow those instructions, then rerun the process with `--retry.` NOTE: `--retry` is currently tech preview and not thoroughly tested.

## Connecting to HS2 via Kerberos

Connecting to an HDP cluster running 2.6.5 with Binary protocol and Kerberos triggers an incompatibility issue:
`Unrecognized Hadoop major version number: 3.1.1.7.1....`

**Solution**

The application is built with CDP libraries (excluding the Hive Standalone Driver).  When `Kerberos is the `auth` protocol to connect to **Hive 1**, it will get the application libs which will NOT be compatible with the older cluster.

Kerberos connections are only supported to the CDP cluster.

When connecting via `Kerberos, you will need to include the `--hadoop-classpath` when launching `hms-mirror`.

## Auto Partition Discovery not working

I've set the `partitionDiscovery:auto` to `true,` but the partitions aren't getting discovered.

**Solution**

In CDP Base/PVC versions < 7.1.6 have not set the housekeeping thread that runs to activate this feature.

In the Hive metastore configuration in Cloudera Manager, set `metastore.housekeeping.threads.on=true` in the _Hive Service Advanced Configuration Snippet (Safety Valve) for hive-site.xml_

![pic](images/hms_housekeeping_thread.png)

## Hive SQL Exception / HDFS Permissions Issues

```
Caused by: java.lang.RuntimeException: org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAccessControlException:Permission denied: user [dstreev] does not have [ALL] privilege on [hdfs://HDP50/apps/hive/warehouse/tpcds_bin_partitioned_orc_10.db/web_site]
```

This error is a permission error to HDFS.  For HYBRID, EXPORT_IMPORT, SQL, and SCHEMA_ONLY (with `-ams` enabled), this could be an issue with cross-cluster HDFS access.

Review the output report for details of where this error occurred (LEFT or RIGHT cluster).

When dealing with CREATE DDL statements submitted through HS2 with a `LOCATION` element in them, the submitting *user* **AND** the HS2 *service account* must have permissions to the directory.  Remember, with cross-cluster access, the user identity will originate on the RIGHT cluster and will be **EVALUATED** on the LEFT clusters storage layer.

For migrations, the `hms-mirror` running user (JDBC) and keytab user (HDFS) should be privileged users.

### Example and Ambari Hints

After checking permissions of 'dstreev': Found that the 'dstreev' user was NOT the owner of the files in these directories on the LEFT cluster. The user running the process needs to be in 'dfs.permissions.superusergroup' for the lower clusters 'hdfs' service.  Ambari 2.6 has issues setting this property: https://jira.cloudera.com/browse/EAR-7805

Follow the workaround above or add the user to the 'hdfs' group. Or use Ranger to allow all access. On my cluster, with no Ranger, I had to use '/var/lib/ambari-server/resources/scripts/configs.py' to set it manually for Ambari.

`sudo ./configs.py --host=k01.streever.local --port=8080 -u admin -p admin -n hdp50 -c hdfs-site -a set -k dfs.permissions.superusergroup -v hdfs_admin`


## YARN Submission stuck in ACCEPTED phase

The process uses a connection pool to hive.  If the concurrency value for the cluster is too high, you may have reached the maximum ratio of AM (Application Masters) for the YARN queue.

Review the ACCEPTED jobs and review the jobs *Diagnostics* status for details on _why_ the jobs is stuck.

**Solution**

Either of:
1. Reduce the concurrency in the configuration file for `hms-mirror`
2. Increase the AM ratio or Queue size to allow the jobs to be submitted.  This can be done while the process is running.

## Spark DFS Access

If you have problems accessing HDFS from `spark-shell` or `spark-submit` try adding the following configuration to spark:

```
--conf spark.yarn.access.hadoopFileSystems=hdfs://<NEW_NAMESPACE>,hdfs://<OLD_NAMESPACE>
```

## Permission Issues

`HiveAccessControlException Permission denied user: [xxxx] does not have [ALL] privileges on ['location'] [state=42000,code=40000]`

and possibly

In HS2 Logs: `Unauthorized connection for super-user`

**Solution**

Caused by the following:
- The 'user' doesn't have access to the location as indicated in the message.  Verify through 'hdfs' that this is true or not. If the user does NOT have access, grant them access and try again.
- The 'hive' service account running HS2 does NOT have access to the location.  The message will mask this and present it as a 'user' issue, when it is in fact an issue with the 'hive' service account.  Grant the account the appropriate access.
- The 'hive' service does NOT have proxy permissions to the storage layer.
    - Check the `hadoop.proxyuser.hive.hosts|groups` setting in `core-site.xml`.  If you are running into this `super-user` error on the RIGHT cluster, while trying to access a storage location on the *LEFT* cluster, ensure the proxy settings include the rights values in the RIGHT clusters `core-site.xml`, since that is where HS2 will pick it up from.

## Must use HiveInputFormat to read ACID tables

We've seen this while attempting to migrate ACID tables from older clusters (HDP 2.6).  The error occurs when we try to extract the ACID table data to a 'transfer' external table on the LEFT cluster, which is 'legacy'.

**Solution**

HDP 2.6.5, the lowest supported cluster version intended for this process, should be using the 'tez' execution engine  `set hive.execution.engine=tez`.  If the cluster has been upgraded from an older HDP version OR they've simply decided NOT to use the `tez` execution engine', you may get this error.

In `hms-mirror` releases 1.3.0.5 and above, we will explicitly run `set hive.execution.engine=tez` on the LEFT cluster when identified as a 'legacy' cluster.  For version 1.3.0.4 (the first version to support ACID transfers), you'll need to set the hive environment for the HS2 instance you're connecting to use `tez` as the execution engine.

## ACL issues across cross while using LOWER clusters storage

Are you seeing something like this?

```
org.apache.hadoop.hive.ql.ddl.DDLTask. MetaException(message:Got exception: org.apache.hadoop.security.AccessControlException Permission denied: user=hive, access=WRITE, inode="/apps/hive/warehouse/merge_files.db/merge_files_part_a_small_replacement":dstreev:hadoop:drwxr-xr-x at
```

This is caused when trying to `CREATE` a table on the **RIGHT** cluster that references data on the **LEFT** cluster.  When the LEFT cluster is setup differently with regard to impersonation (doas) than the RIGHT, transfer tables are created with POSIX permissions that may not allow the RIGHT cluster/user to access that location.

**Solution**

Using Ranger on the LEFT cluster, open up the permissions to allow the requesting user access as identified.

