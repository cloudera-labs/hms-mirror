# Issues

## Table processing completed with `ERROR`

We make various checks as we perform the migrations and when those check don't pass, the result is an error.

### Solution

In [tips](./running_tips.md) we suggest running with `--dryrun` first.  This will catch the potential issues first, without taking a whole lot of time.  Use this to remediate issues before executing.

If the scenario that cause the `ERROR` is known, a remediation summary will be in the output report under **Issues** for that table.  Follow those instructions than rerun the process with `--retry`.

## Connecting to HS2 via Kerberos

Connecting to an HDP cluster running 2.6.5 with Binary protocol and Kerberos triggers an incompatibility issue:
`Unrecognized Hadoop major version number: 3.1.1.7.1....`

### Solution

The application is built with CDP libraries (excluding the Hive Standalone Driver).  When `kerberos` is the `auth` protocol to connect to **Hive 1**, it will get the application libs which will NOT be compatible with the older cluster.

Kerberos connections are only supported to the CDP cluster.

When connecting via `kerberos`, you will need to include the `--hadoop-classpath` when launching `hms-mirror`.     

## Auto Partition Discovery not working

I've set the `partitionDiscovery:auto` to `true` but the partitions aren't getting discovered.

### Solution

In CDP Base/PVC versions < 7.1.6 have not set the house keeping thread that runs to activate this feature.

In the Hive metastore configuration in Cloudera Manager set `metastore.housekeeping.threads.on=true` in the _Hive Service Advanced Configuration Snippet (Safety Valve) for hive-site.xml_

![pic](./images/hms_housekeeping_thread.png)

## HDFS Permissions Issues

Caused by: java.lang.RuntimeException: org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAccessControlException:Permission denied: user [dstreev] does not have [ALL] privilege on [hdfs://HDP50/apps/hive/warehouse/tpcds_bin_partitioned_orc_10.db/web_site]

After checking permissions of 'dstreev': Found that the 'dstreev' user was NOT the owner of the files in these directories on the LOWER cluster. The user running the process needs to be in 'dfs.permissions.superusergroup' for the lower clusters 'hdfs' service.  Ambari 2.6 has issues setting this property: https://jira.cloudera.com/browse/EAR-7805

Follow workaround above or add user to the 'hdfs' group. Or use Ranger to allow all access. On my cluster, with no Ranger, I had to use '/var/lib/ambari-server/resources/scripts/configs.py' to set it manually for Ambari.

`sudo ./configs.py --host=k01.streever.local --port=8080 -u admin -p admin -n hdp50 -c hdfs-site -a set -k dfs.permissions.superusergroup -v hdfs_admin`

## YARN Submission stuck in ACCEPTED phase

The process uses a connection pool to Hive.  If concurrency value for the cluster is too high, you may have reached the maximum ratio of AM (Application Masters) for the YARN queue.

Review the ACCEPTED jobs and review the jobs *Diagnostics* status for details on _why_ the jobs is stuck.

### Solution

Either of:
1. Reduce the concurrency in the configuration file for `hms-mirror`
2. Increase the AM ratio or Queue size to allow the jobs to be submitted.  This can be done while the process is running.

## Spark DFS Access

If you have problems accessing HDFS from `spark-shell` or `spark-submit` try adding the following configuration to spark:

```
--conf spark.yarn.access.hadoopFileSystems=hdfs://<NEW_NAMESPACE>,hdfs://<OLD_NAMESPACE>
```
