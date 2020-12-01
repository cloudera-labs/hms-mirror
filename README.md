# HMS-Mirror

[Getting / Building / Running HMS Mirror](./setup.md)

[Running HMS Mirror](./running.md)

This process uses the 'storage' layer of a legacy cluster (LOWER) from the compute layer of a modern cluster (UPPER).

We treat hdfs on the **LOWER** cluster as a shared resource and migrate the metadata for Hive over to the **UPPER** cluster using _standard_ `hive` tools and techniques.

This shared storage approach, with replicated metadata, should address a large majority of `hive metastore` analytic use cases.  This includes work from Hive, Spark, and other technologies integrated with the `hive metastore`.

This process addresses `EXTERNAL` and **LEGACY** `MANAGED` tables.  It does **NOT** support `ACID` transactional tables.

The _upper_ cluster should be used as a **READ-ONLY** view of the data, until _Stage 2_ of the plan is implemented.

![hms-mirror](./images/HMS-Mirror.png)

## Stage 1 - Use Data In place

On the Lower Cluster we need to extract metadata for the tables we want to use in the upper cluster.  There are a few limitations to note here:
- We're NOT sharing metadata stores between the clusters
- This technique is restricted to _external_ and _non-transactional managed_ tables.  **ACID Tables** are **NOT** supported.
- Partitioned datasets will NOT be managed automatically in the upper clusters metastore during Stage 1.  They need to be updated manually with a tool like `msck`.

- [Link Clusters](./link_clusters.md) so the Upper Cluster can distcp from the lower cluster.

The following is an outline of the technique used for the "METADATA" stage, but may not reflect that exact sequence taken as we've tested and adjusted the process.

### From the **Lower** Cluster:
- Create a transfer db to store empty table definitions.

```
CREATE DATABASE IF NOT EXISTS transfer_<abc>;
USE transfer_<abc>;
```

- Create an empty copy of the table.

> This creates an 'empty' shell for that tables we want to share with the upper cluster.

```
CREATE TABLE <abc> LIKE <orig_db>.<abc>;
```

- Export the empty table with standard Hive tooling.   We use the 'empty' table concept to eliminate to 'data' transfer process.

```
EXPORT TABLE <abc> TO '<hdfs_location_on_lower_cluster>';
```


### From the **UPPER** Cluster
- Create a matching database.

```
CREATE DATABASE IF NOT EXISTS <orig_db_name>;
USE <orig_db_name>;
```

- IMPORT from the **lower cluster location**. Use the original database and table name to keep all jobs scripts run on the upper cluster in sync without location adjustments.

```
IMPORT EXTERNAL TABLE <orig_table_name> FROM '<hdfs_location_on_lower_cluster>';
```

- Alter new table metadata to point to **lower cluster storage**

```
ALTER TABLE <orig_table_name> SET LOCATION '<full_hdfs_uri_on_lower_cluster>';
```

- For Partitioned Tables, build the partitions in the **upper clusters** metastore

```
MSCK REPAIR TABLE <orig_table_name>;
```

- Sample table for data.

```
-- If partitioned
SHOW PARTITIONS <orig_table_name>;
-- Sample Records
SELECT * FROM <orig_table_name> LIMIT 10;
```

## Stage 2 - Finalized Data Transfer (WIP)

This step migrates data over from the _lower_ cluster to the _upper_ cluster without current jobs running on the upper cluster requiring rework.

All actions here are performed on the **upper** cluster.

- Make a copy of the migrated table that's pointing to the _lower_ cluster storage.

```
CREATE TABLE <orig_table>_transition LIKE <orig_table>;
```

- Set the location of the _transition_ table to the same path as the original on the _lower_ cluster, BUT **WITHOUT** the _lower_ clusters hdfs protocol.  For example, less assume the original location was `hdfs://HDP50/apps/hive/warehouse/abc.db/xyz` .  The new location for the 'transfer' table should be `/apps/hive/warehouse/abc.db/xyz`

> While the table name for this part is temporary, the location is _NOT_.  That's important, which you'll see after the process has been completed.

- Migrate data, through Hive SQL.  NOTE: For partitioned LARGE partitioned datasets, a transition plan that controls the migration of partitions needs to be created.

```
FROM <orig_table>
INSERT INTO <orig_table>_transition 
SELECT
  *;
```

- Shuffle tables.  This process isn't quite atomic, but it is much quicker than trying to build the final table in-place.

```
ALTER TABLE <orig_table> TO <orig_table>_legacy;
ALTER TABLE <orig_table>_transition TO <orig_table>;
```

- Validate Locations and Data

```
SHOW CREATE TABLE <orig_table>;
SELECT * FROM <orig_table> LIMIT 10;
```


- Clean up.  The `<orig_table>_legacy` points to the legacy clusters hdfs location AND should be defined as an `EXTERNAL` table without `PURGE`.  Remove the legacy metadata.

```
DROP TABLE <orig_table>_legacy;
```







