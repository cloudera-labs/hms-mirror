# Strategies

| | Schema Migration | Data Migration                                                                                                                        |
|:---|:---|:--------------------------------------------------------------------------------------------------------------------------------------|
| SCHEMA_ONLY | X | via `-dc                                                                                                                              |--distcp` |
| LINKED | X | no data movement.  RIGHT leverages data on the LEFT cluster.  Requires the clusters to be [linked](Linking-Cluster-Storage-Layers.md) |
| DUMP | X |                                                                                                                                       |
| SQL | X | X                                                                                                                                     |
| EXPORT_IMPORT | X | X                                                                                                                                     |
| HYBRID | X | X                                                                                                                                     |
| STORAGE_MIGRATION | X | X                                                                                                                                     |


