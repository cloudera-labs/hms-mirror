# CONVERT_LINKED

If you migrated schemas with the [Linked](LINKED.md) strategy and don't want to drop the database and run SCHEMA_ONLY to adjust the locations from the LEFT to the RIGHT cluster, use this strategy to make the adjustment.

This will work only on tables that were migrated already.  It will reset the location to the same relative location on the RIGHT clusters `hcfsNamespace`.  This will also check to see if the table was a 'legacy' managed table and set the `external.table.purge` flag appropriately.  Tables that are 'partitioned' will be handled differently, since each partition has a reference to the 'linked' location.  Those tables will first be validated that they NOT `external.table.purge`.  If they are, that property will 'UNSET'.  Then the table will be dropped, which will remove all the partition information.  Then they'll be created again and `MSCK` will be used to discover the partitions again on the RIGHT clusters namespace.

This process does NOT move data.  It expects that the data was migrated in the background, usually by `distcp` and has been placed in the same relative locations on the RIGHT clusters `hcfsNameSpace`.

This process does NOT work for ACID tables.

AVRO table locations and SCHEMA location and definitions will be changed and copied.
