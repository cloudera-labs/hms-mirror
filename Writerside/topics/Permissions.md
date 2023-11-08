# Permissions

In both the METADATA and STORAGE phases of `hms-mirror` the RIGHT cluster will reach down into the LEFT clusters storage layer to either _use_ or _copy_ the data.

`hms-mirror` access each cluster via JDBC and use the RIGHT cluster for *storage* layer access.

When the RIGHT cluster is using 'non-impersonation' (hive `doas=false`), the *hive* service account on the **RIGHT** cluster (usually `hive`) needs access to the storage layer on the **LEFT** cluster to use this data to support sidecar testing, where we use the data of the LEFT cluster but *mirror* the metadata.

> Having Ranger on both clusters helps because you can create additional ACLs to provide the access required.

**OR**

>Checked permissions of '<submitting_user>': Found that the '<submitting_user>' user was NOT the owner of the files in these directories. The user running the process needs to be in 'dfs.permissions.superusergroup' for the LEFT clusters 'hdfs' service.  Ambari 2.6 has issues setting this property: https://jira.cloudera.com/browse/EAR-7805

<note>
 Follow workaround above or add user to the 'hdfs' group. I had to use '/var/lib/ambari-server/resources/scripts/configs.py' to set it manually for Ambari.
</note>

 `sudo ./configs.py --host=k01.streever.local --port=8080 -u admin -p admin -n hdp50 -c hdfs-site -a set -k dfs.permissions.superusergroup -v hdfs_admin`
