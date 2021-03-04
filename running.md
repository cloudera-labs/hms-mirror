# Running HMS Mirror

After running the `setup.sh` script, `hms-mirror` will be available in the `$PATH` in a default configuration.

## Assumptions

1. This process will only 'migrate' EXTERNAL and MANAGED (non-ACID/Transactional) tables.
2. MANAGED tables replicated to the **RIGHT** cluster will be converted to "EXTERNAL" tables for the 'metadata' stage.  They will be tagged as 'legacy managed' in the **RIGHT** cluster and will NOT be assigned the `external.table.purge=true` flag, yet.  Once the table's data has been migrated, the table will be adjusted to be purgeable via `external.table.purge=true` to match the classic `MANAGED` table behavior.
1. The **RIGHT** cluster has 'line of sight' to the **LEFT** cluster.
2. The **RIGHT** cluster has been configured to access the **LEFT** cluster storage. See [link clusters](./link_clusters.md).  This is the same configuration that would be required to support `distcp` from the **RIGHT** cluster to the **LEFT** cluster.
3. The movement of metadata/data is from the **LEFT** cluster to the **RIGHT** cluster.
4. With Kerberos, each cluster must share the same trust mechanism.
    - The **RIGHT** cluster must be Kerberized IF the **LEFT** cluster is.
    - The **LEFT** cluster does NOT need to be kerberized if the **RIGHT** cluster is kerberized.
7. The **LEFT* cluster does NOT have access to the **RIGHT** cluster.
8. The credentials use by 'hive' (doas=false) in the **RIGHT** cluster must have access to the required storage (hdfs) locations on the lower cluster.
    - If the **RIGHT** cluster is running impersonation (doas=true), that user must have access to the required storage (hdfs) locations on the lower cluster.

**HELP**
```
usage: hms-mirror
                  version: ....
 -accept,--accept                            Accept ALL confirmations and
                                             silence prompts
 -cfg,--config <filename>                    Config with details for the
                                             HMS-Mirror.  Default:
                                             $HOME/.hms-mirror/cfg/default
                                             .yaml
 -d,--data <strategy>                        Specify how the data will
                                             follow the schema.
                                             [SCHEMA_ONLY, LINKED, SQL,
                                             EXPORT_IMPORT, HYBRID,
                                             INTERMEDIATE, COMMON]
 -db,--database <databases>                  Comma separated list of
                                             Databases (upto 100).
 -dbp,--db-prefix <prefix>                   Optional: A prefix to add to
                                             the RIGHT cluster DB Name.
                                             Usually used for testing.
 -e,--execute                                Execute actions request,
                                             without this flag the process
                                             is a dry-run.
 -h,--help                                   Help
 -is,--intermediate-storage <storage-path>   Intermediate Storage used
                                             with Data Strategy
                                             INTERMEDIATE.
 -o,--output-dir <outputdir>                 Output Directory (default:
                                             $HOME/.hms-mirror/reports/<yy
                                             yy-MM-dd_HH-mm-ss>
 -r,--retry                                  Retry last incomplete run for
                                             'cfg'.  If none specified,
                                             will check for 'default'
 -ro,--read-only                             For SCHEMA_ONLY, COMMON, and
                                             LINKED data strategies set
                                             RIGHT table to NOT purge on
                                             DROP
 -sql,--sql-output                           Output the SQL to the report
 -tf,--table-filter <regex>                  Filter tables with name
                                             matching RegEx
```

## Examples

### Replicating 2 database definitions

`hms-mirror -db my_first_db,my_second_db`

