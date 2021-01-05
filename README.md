# HMS-Mirror

"hms-mirror" is a utility used to bridge the gap between two clusters and migrate `hive` metadata AND the underlying **data**.

[Getting / Building / Running HMS Mirror](./setup.md)

[Running HMS Mirror](./running.md)

[What does hms-mirror do? (Design-Spec)](./design-spec.md)

This process uses the 'storage' layer of a legacy cluster (LOWER) from the compute layer of a modern cluster (UPPER).

We treat hdfs on the **LOWER** cluster as a shared resource and migrate the metadata for Hive over to the **UPPER** cluster using _standard_ `hive` tools and techniques.

This shared storage approach, with replicated metadata, should address a large majority of `hive metastore` analytic use cases.  This includes work from Hive, Spark, and other technologies integrated with the `hive metastore`.

This process addresses `EXTERNAL` and **LEGACY** `MANAGED` tables.  It does **NOT** support `ACID` transactional tables.

The _upper_ cluster should be used as a **READ-ONLY** view of the data, until _Stage 2_ of the plan is implemented.

![hms-mirror](./images/HMS-Mirror.png)

## Phases
### METADATA

This phase is used to copy metadata from a 'source' cluster to a 'target' cluster while leveraging the data on the lower cluster.  The scenario 'can' be used to migrate schema data from other 'hive metastore' systems to CDP Cloud as well.

There are a few limitations to note here:
- We're NOT sharing metadata stores between the clusters
- This technique is restricted to _external_ and _non-transactional managed_ tables.  **ACID Tables** are **NOT** supported.
- Partitioned datasets can be 'auto discovered' when the configuration `partitionDiscovery:auto=true` is set.
- Partitions that do NOT follow the standard directory structure and aren't discoverable via `msck` are NOT supported yet.

- [Link Clusters](./link_clusters.md) so the Upper Cluster can distcp from the lower cluster.  This is required to support storage access from the 'target' cluster to the 'source' cluster. The user/service account used to launch the jobs in the 'target' cluster is translated to the 'source' clusters storage access layer.  The appropriate ACL's on the 'source' cluster must be established to allow this connection.

### STORAGE

This phase is used to copy the _metadata_ AND _data_ from the 'source' cluster to the 'target' cluster.  There are several transfer options available:

- SQL - Use `hive` to migrate the data between clusters using a series of transfer tables and _rename_ semantics to minimize disruption and reduce the number of times data is moved.
- EXPORT_IMPORT - Use 'hive' EXPORT / IMPORT built in processes to create a copy of the data then transfer it to the target cluster.  NOTE: For datasets with a high number of partitions, the EXPORT/IMPORT process can be quite time-consuming.  Consider using 'SQL' or 'DISTCP' methods.
- DISTCP - TBD.





