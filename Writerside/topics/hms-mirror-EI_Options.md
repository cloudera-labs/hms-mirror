# Options

## `-ep|--export-partition-count <limit>` 

Sets the limit for the number of partitions to use EXPORT_IMPORT.  The default is 100.  When a table has a partition count that exceeds this value, the [SQL](hms-mirror-sql.md) strategy will be used.

To persist a higher value without specifying the `-ep` option, add the following to the [config](hms-mirror-Default-Configuration-Template.md).

```yaml
hybrid:
  exportImportPartitionLimit: <limit>
```
