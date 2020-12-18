# HMS-Mirror

[Getting / Building / Running HMS Mirror](./setup.md)

[Running HMS Mirror](./running.md)

[What does hms-mirror do? (Design-Spec)](./design-spec.md)

This process uses the 'storage' layer of a legacy cluster (LOWER) from the compute layer of a modern cluster (UPPER).

We treat hdfs on the **LOWER** cluster as a shared resource and migrate the metadata for Hive over to the **UPPER** cluster using _standard_ `hive` tools and techniques.

This shared storage approach, with replicated metadata, should address a large majority of `hive metastore` analytic use cases.  This includes work from Hive, Spark, and other technologies integrated with the `hive metastore`.

This process addresses `EXTERNAL` and **LEGACY** `MANAGED` tables.  It does **NOT** support `ACID` transactional tables.

The _upper_ cluster should be used as a **READ-ONLY** view of the data, until _Stage 2_ of the plan is implemented.

![hms-mirror](./images/HMS-Mirror.png)

## Stage 1 - METADATA

On the Lower Cluster we need to extract metadata for the tables we want to use in the upper cluster.  There are a few limitations to note here:
- We're NOT sharing metadata stores between the clusters
- This technique is restricted to _external_ and _non-transactional managed_ tables.  **ACID Tables** are **NOT** supported.
- Partitioned datasets can be 'auto discovered' when the configuration `partitionDiscovery:auto=true` is set.
- Partitioned datasets can be 'auto discovered' when the configuration `partitionDiscovery:auto=true` is set.
  
- Partitions that do NOT follow the standard directory structure and aren't discoverable via `msck` are NOT supported yet.

- [Link Clusters](./link_clusters.md) so the Upper Cluster can distcp from the lower cluster.


## Stage 2 - STORAGE






