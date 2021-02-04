### METADATA

This process uses the 'storage' layer of a legacy cluster (LOWER) from the compute layer of a modern cluster (UPPER).

We treat hdfs on the **LOWER** cluster as a shared resource and migrate the metadata for Hive over to the **UPPER** cluster using _standard_ `hive` tools and techniques.

This shared storage approach, with replicated metadata, should address a large majority of `hive metastore` analytic use cases.  This includes work from Hive, Spark, and other technologies integrated with the `hive metastore`.

This process addresses `EXTERNAL` and **LEGACY** `MANAGED` tables.  It does **NOT** support `ACID` transactional tables.

The _upper_ cluster should be used as a **READ-ONLY** view of the data, until _Stage 2_ of the plan is implemented.

![hms-mirror](./images/HMS-Mirror.png)

This is helpful for:
- Testing a new cluster without having to migrate data.
- Testing data workflows and applications in a *test* cluster using the data in a *production* cluster.
- Migrating hive schemas from EMR/Dataproc/HDi to CDP Cloud.

There are a few limitations to note here:
- We're NOT sharing metadata stores between the clusters
- This technique is restricted to _external_ and _non-transactional managed_ tables.  **ACID Tables** are **NOT** supported.
- Partitioned datasets can be 'auto discovered' when the configuration `partitionDiscovery:auto=true` is set.
- Partitions that do NOT follow the standard directory structure and aren't discoverable via `msck` are NOT supported yet.
- Connections to HS2 should avoid using ZooKeeper Discovery especially for _LOWER_ clusters.  Use direct connections or a proxy connection through KNOX to reduce the dependencies.
- `hms-mirror` can only support _kerberos_ Authentication modes to HS2 for Hive 3.  Which means that the _LOWER_ cluster HS2 JDBC URL should NOT use `kerberos` as the authentication model. Using any other authentication model is supported.

- [Link Clusters](./link_clusters.md) so the Upper Cluster can distcp from the lower cluster.  This is required to support storage access from the 'target' cluster to the 'source' cluster. The user/service account used to launch the jobs in the 'target' cluster is translated to the 'source' clusters storage access layer.  The appropriate ACL's on the 'source' cluster must be established to allow this connection.

#### Strategies

Strategies are the varies techniques used to migrate hive schemas.

##### DIRECT
This will extract a copy of the METADATA from the _LOWER_ cluster, make any adjustments required for compatibility, and replay the DDL on the _UPPER_ cluster.

See [ownership](#ownership) for managed data.

##### EXPORT_IMPORT
This will create a transition temp table in the lower cluster based on the source table, but with no data in it.  Then use hive EXPORT/IMPORT processes to recreate the table in the _target_ cluster.  Once created there, it will adjust the location to look at the data in the _source_ cluster.

See [ownership](#ownership) for managed data.

##### SCHEMA_EXTRACT
WIP

##### DISTCP
This assumes the data has been moved from the _source_ cluster to the _target_ cluster with DISTCP.  This will copy the schema as in "DIRECT", but will adjust the location to the _target_ clusters namespace using the SAME relative location as was originally used in the _source_ cluster.

Ownership will be translated in this case because the data is NOT shared with the lower cluster, UNLESS the `-dr` or `--disaster-recovery` flag is set.  In this case, we will NOT translate ownership, since this is a _read only_ copy and we're relying on `distcp` to maintain a consistent view of the data in the _source_ cluster.

#### Ownership

Migration schemas from one cluster to another while sharing the data on one of the clusters creates a dilemma of _ownership_.  In practice only one schema can _own_ the data.  

What does _own_ mean in this case?  If the table was _managed_, then the data is dropped when the table is dropped.  So if we haven't committed to using the _UPPER_ cluster and are simply testing, we want to be careful regarding the effects of dropping the table in the _UPPER_ cluster on the data.  We certainly don't want to delete that data inadvertently.

By specifying the `-c` (commit) option we will ensure the _UPPER_ cluster _owns_ that data "IF" the table was originally managed in the lower cluster.  The `--commit` option is really only relevant when using _shared storage_ between the two clusters.  This could be a cloud bucket, Isilon, or other blob storage medium.  To `--commit` ownership to the _UPPER_ cluster, _shared storage_ is required an you need to also specify `-ss` or `--shared-storage`.