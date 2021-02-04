# Linking Clusters Storage Layers

For the `hms-mirror` process to work, it relies on the UPPER clusters ability to _SEE_ and _ACCESS_ data in the LOWER clusters HDFS namespace.  This is the same access/configuration required to support DISTCP.

Access is required by the UPPER cluster HCFS namespace to the LOWER clusters HCFS namespace.  UPPER clusters with a greater HDFS version support **LIMITED** functionality for data access in the LOWER cluster.

NOTE: This isn't designed to be a permanent solution and should only be used for testing and migration purposes.

## Goal

What does it take to support HDFS visibility between these two clusters?

Can that integration be used to support the Higher Clusters use of the Lower Clusters HDFS Layer for distcp AND Hive External Table support?

## Scenario #1

### HDP 2.6.5 (Hadoop 2.7.x)
Kerberized - sharing same KDC as CDP Base Cluster

#### Configuration Changes

The _namenode_ *kerberos* principal MUST be changed from `nn` to `hdfs` to match the namenode principal of the CDP cluster.  

Note: You may need to add/adjust the `auth_to_local` settings to match this change. 

If this isn't done, `spark-shell` and `spark-submit` will fail to initialize.  When changing this in Ambari on HDP, you will need to *reset* the HDFS zkfc `ha` zNode in Zookeeper and reinitialize the hdfs `zkfc`.

From a Zookeeper Client: `/usr/hdp/current/zookeeper-client/bin/zkCli.sh -server localhost`
```
rmr /hadoop-ha
```

Initialize zkfc
```
hdfs zkfc -formatZK
```

_hdfs-site.xml_
```
hadoop.rpc.protection=true
```

_core-site.xml_
```
dfs.encrypt.data.transfer=true
dfs.encrypt.data.transfer.algorithm=3des
dfs.encrypt.data.transfer.cipher.key.bitlength=256
```

### CDP 7.1.4 (Hadoop 3.1.x)
Kerberized, TLS Enabled

#### Configuration Changes

Requirements that allow this (upper) cluster to negotiate and communicate with the lower environment.

_Cluster Wide hdfs-site.xml Safety Value_

```
ipc.client.fallback-to-simple-auth-allowed=true
```

_HDFS Service Advanced Config hdfs-site.xml_

```
# For this Clusters Name Service
dfs.internal.nameservices=HOME90

# For the target (lower) environment HA NN Services
dfs.ha.namenodes.HDP50=nn1,nn2
dfs.namenode.rpc-address.HDP50.nn1=k01.streever.local:8020
dfs.namenode.rpc-address.HDP50.nn2=k02.streever.local:8020
dfs.namenode.http-address.HDP50.nn1=k01.streever.local:50070
dfs.namenode.http-address.HDP50.nn2=k02.streever.local:50070
dfs.namenode.https
 address.HDP50.nn1=k01.streever.local:50471
dfs.namenode.https-address.HDP50.nn2=k02.streever.local:50470
dfs.client.failover.proxy.provider.HDP50=org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider

# For Available Name Services
dfs.nameservices=HOME90,HDP50  
```

### Running `distcp` from the **UPPER** Cluster

NOTE: Running `distcp` from the **LOWER** cluster isn't supported since the `hcfs client` is not forward compatible.

Copy 'from' Lower Cluster

```
hadoop distcp hdfs://HDP50/user/dstreev/sstats/queues/2020-10.txt /user/dstreev/temp
```

Copy 'to' Lower Cluster

```
hadoop distcp /warehouse/tablespace/external/hive/cvs_hathi_workload.db/queue/2020-10.txt hdfs://HDP50/user/dstreev/temp
```

### Sourcing Data from Lower Cluster to Support Upper Cluster External Tables

#### Permissions
The lower cluster must allow the upper clusters _Hive Server 2_ host as a 'hive' proxy.  The setting in the lower clusters _custom_ `core-site.xml` may limit this to that clusters (lower) HS2 hosts.  Open it up to include the upper clusters HS2 host.

_Custom core-site.xml in Lower Cluster_
```
hadoop.proxyuser.hive.hosts=*
```

Credentials from the 'upper' cluster will be projected down to the 'lower' cluster.  This means the `hive` user in the upper cluster, when running with 'non-impersonation' will require access to the datasets in the lower cluster HDFS.

For table creation in the 'upper' clusters Metastore, a permissions check will be done on the lower environments directory for the submitting user.  So, both the service user AND `hive` will require access to the directory location specified in the lower cluster.

When the two clusters _share_ accounts and the same accounts are used between environment for users and service accounts, then access should be simple.

When a different set of accounts are used, the 'principal' from the upper clusters service account for 'hive' and the 'user' principal will be used in the lower cluster.  Which means additional HDFS policies in the lower cluster may be required to support this cross environment work.
