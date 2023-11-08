# EXPORT_IMPORT

We'll use EXPORT_IMPORT to get the data to the new cluster. The default behavior requires the clusters to be [linked](Linking-Cluster-Storage-Layers.md).

EXPORT to a location on the LEFT cluster where the RIGHT cluster can pick it up with IMPORT.

When `-ma` (migrate acid) tables are specified, and the LEFT and RIGHT cluster DON'T share the same 'legacy' setting, we will NOT be able to use the EXPORT_IMPORT process due to incompatibilities between the Hive versions.  We will still attempt to migrate the table definition and data by copying the data to an EXTERNAL table on the lower cluster and expose this as the source for an INSERT INTO the ACID table on the RIGHT cluster.

![export_import](images/sql_exp-imp.png)

If the `-is <intermediate-storage-path>` is used with this option, we will migrate data to this location and use it as a transfer point between the two clusters.  Each cluster will require access (some configuration adjustment may be required) to the location.  In this scenario, the clusters do NOT need to be linked.

![intermediate](images/intermediate.png)
