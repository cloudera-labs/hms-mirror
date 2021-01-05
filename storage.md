## STORAGE

This phase is used to copy the _metadata_ AND _data_ from the 'source' cluster to the 'target' cluster.  There are several transfer options available:

### Strategies

#### SQL
Use `hive` to migrate the data between clusters using a series of transfer tables and _rename_ semantics to minimize disruption and reduce the number of times data is moved.

![sql](./images/StorageSQL.png)

#### EXPORT_IMPORT
Use 'hive' EXPORT / IMPORT built in processes to create a copy of the data then transfer it to the target cluster.  NOTE: For datasets with a high number of partitions, the EXPORT/IMPORT process can be quite time-consuming.  Consider using 'SQL' or 'DISTCP' methods.

![sql](./images/StorageExportImport.png)

#### HYBRID

Allows us to select either SQL or EXPORT/IMPORT depending on the tables partition volumes.  For non-partitioned tables, the SQL strategy will be used.  For partitioned tables, we'll review the number of partitions and compare it to the configuration:
- `storage:hybrid:sqlPartitionLimit` - When exceeded, the EXPORT_IMPORT strategy will be used, otherwise the SQL strategy will be used.

##### DISTCP
TBD.
