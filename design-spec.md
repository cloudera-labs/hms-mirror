# The Process

The describe the process 'in a general sense' what the 'hms-mirror` tools does.  There are several rules built into the process that accounts for the translation of "Legacy" hive tables, not accounted for in this description.

NOTE: You do NOT need to do the following, 'hms-mirror' will do it for you.

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
