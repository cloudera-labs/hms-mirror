# Running HMS Mirror

After running the `setup.sh` script, `hms-mirror` will be available in the `$PATH` in a default configuration.

## Assumptions

1. This process will only 'migrate' EXTERNAL and MANAGED (non-ACID/Transactional) tables.
2. MANAGED tables replicated to the **UPPER** cluster will be converted to "EXTERNAL" tables for the 'metadata' stage.  They will be tagged as 'legacy managed' in the **UPPER** cluster and will NOT be assigned the `external.table.purge=true` flag, yet.  Once the table's data has been migrated, the table will be adjusted to be purgeable via `external.table.purge=true` to match the classic `MANAGED` table behavior.
1. The **UPPER** cluster has 'line of sight' to the **LOWER** cluster.
2. The **UPPER** cluster has been configured to access the **LOWER** cluster storage. See [link clusters](./link_clusters.md).  This is the same configuration that would be required to support `distcp` from the **UPPER** cluster to the **LOWER** cluster.
3. The movement of metadata/data is from the **LOWER** cluster to the **UPPER** cluster.
4. With Kerberos, each cluster must share the same trust mechanism.
    5. The **UPPER** cluster must be Kerberized IF the **LOWER** cluster is.
    6. The **LOWER** cluster does NOT need to be kerberized if the **UPPER** cluster is kerberized.
7. The **LOWER* cluster does NOT have access to the **UPPER** cluster.
8. The credentials use by 'hive' (doas=false) in the **UPPER** cluster must have access to the required storage (hdfs) locations on the lower cluster.
    9. If the **UPPER** cluster is running impersonation (doas=true), that user must have access to the required storage (hdfs) locations on the lower cluster.

**HELP**
```
usage: hive-mirror
 -cfg,--config <arg>    Config with details for the HMS-Mirror.  Default:
                        $HOME/.hms-mirror/cfg/default.yaml
 -db,--database <arg>   Comma separated list of Databases (upto 100).
 -h,--help              Help
 -m,--metastore         Run HMS-Mirror Metadata
 -o,--output-dir        Output Directory (default:
                        $HOME/.hms-mirror/reports/hms-mirror-<stage>-<times
                        tamp>.md
 -s,--storage           Run HMS-Mirror Storage
```

There are 2 stages currently supported by the tool:
- `m|metastore` - Will replicate the metadata and will use the **LOWER** clusters storage for the **UPPER** clusters storage location.
- `s|storage` - Will migrate the data from the **LOWER** cluster to the **UPPER** cluster.

## Examples

### Replicating 2 database definitions

`hms-mirror -m -db my_first_db,my_second_db`

