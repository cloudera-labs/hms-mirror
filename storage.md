### STORAGE

This phase is used to copy the _metadata_ AND _data_ from the 'source' cluster to the 'target' cluster.  There are several transfer options available:

#### Strategies

##### SQL
Use `hive` to migrate the data between clusters using a series of transfer tables and _rename_ semantics to minimize disruption and reduce the number of times data is moved.

##### EXPORT_IMPORT
Use 'hive' EXPORT / IMPORT built in processes to create a copy of the data then transfer it to the target cluster.  NOTE: For datasets with a high number of partitions, the EXPORT/IMPORT process can be quite time-consuming.  Consider using 'SQL' or 'DISTCP' methods.

##### DISTCP
TBD.
