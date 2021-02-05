# Running HMS Mirror

After running the `setup.sh` script, `hms-mirror` will be available in the `$PATH` in a default configuration.

## Assumptions

1. This process will only 'migrate' EXTERNAL and MANAGED (non-ACID/Transactional) tables.
2. MANAGED tables replicated to the **RIGHT** cluster will be converted to "EXTERNAL" tables for the 'metadata' stage.  They will be tagged as 'legacy managed' in the **RIGHT** cluster and will NOT be assigned the `external.table.purge=true` flag, yet.  Once the table's data has been migrated, the table will be adjusted to be purgeable via `external.table.purge=true` to match the classic `MANAGED` table behavior.
1. The **RIGHT** cluster has 'line of sight' to the **LEFT** cluster.
2. The **RIGHT** cluster has been configured to access the **LEFT** cluster storage. See [link clusters](./link_clusters.md).  This is the same configuration that would be required to support `distcp` from the **RIGHT** cluster to the **LEFT** cluster.
3. The movement of metadata/data is from the **LEFT** cluster to the **RIGHT** cluster.
4. With Kerberos, each cluster must share the same trust mechanism.
    5. The **RIGHT** cluster must be Kerberized IF the **LEFT** cluster is.
    6. The **LEFT** cluster does NOT need to be kerberized if the **RIGHT** cluster is kerberized.
7. The **LEFT* cluster does NOT have access to the **RIGHT** cluster.
8. The credentials use by 'hive' (doas=false) in the **RIGHT** cluster must have access to the required storage (hdfs) locations on the lower cluster.
    9. If the **RIGHT** cluster is running impersonation (doas=true), that user must have access to the required storage (hdfs) locations on the lower cluster.

**HELP**
```
usage: hms-mirror
                  version:1.2.1.7-SNAPSHOT
 -a,--acid                               ACID table Migration.  Only
                                         supported in the STORAGE stage
 -accept,--accept                        Accept ALL confirmations and
                                         silence prompts
 -c,--commit                             Commit to RIGHT. Applies to
                                         METADATA stage when you want the
                                         RIGHT cluster to 'own' the data
                                         ('external.table.purge'='true')
 -cfg,--config <filename>                Config with details for the
                                         HMS-Mirror.  Default:
                                         $HOME/.hms-mirror/cfg/default.yam
                                         l
 -db,--database <databases>              Comma separated list of Databases
                                         (upto 100).
 -dbp,--db-prefix <prefix>               A prefix to add to the RIGHT
                                         cluster DB Name.
 -dr,--disaster-recovery                 Used for Disaster Recovery.  We
                                         will NOT assign ownership, since
                                         the DR cluster is a Read-Only
                                         Cluster.  Only valid with the
                                         METADATA stage.
 -e,--execute                            Execute actions request, without
                                         this flag the process is a
                                         dry-run.
 -f,--output-file <filename>             Output Directory (default:
                                         $HOME/.hms-mirror/reports/hms-mir
                                         ror-<stage>-<timestamp>.md
 -h,--help                               Help
 -m,--metadata <strategy>                Run HMS-Mirror Metadata with
                                         strategy:
                                         DIRECT(default)|EXPORT_IMPORT|SCH
                                         EMA_EXTRACT
 -r,--retry                              Retry last incomplete run for
                                         'cfg'.  If none specified, will
                                         check for 'default'
 -rs,--replication-strategy <strategy>   Replication Strategy for
                                         Metadata: OVERWRITE|SYNCHRONIZE
                                         (default: SYNCHRONIZE)
 -s,--storage <strategy>                 Run HMS-Mirror Storage with
                                         strategy:
                                         SQL|EXPORT_IMPORT|HYBRID(default)
                                         |DISTCP
 -ss,--share-storage                     Share Storage is used.  Do NOT
                                         adjust protocol namespace after
                                         transferring schema.
 -tf,--table-filter <regex>              Filter tables with name matching
                                         RegEx
```

There are 2 stages currently supported by the tool:
- `m|metastore` - Will replicate the metadata and will use the **LEFT** clusters storage for the **RIGHT** clusters storage location.
- `s|storage` - Will migrate the data from the **LEFT** cluster to the **RIGHT** cluster.

## Application Flag Description

| Flag | Argument(s) | Notes |
|:---|:---|:---|
| `-a` `--acid` | na | When running the `-s` (storage) option with either an `EXPORT_IMPORT` or `HYBRID` strategy you can migrate Transactional/ACID table. This is a *ONE* time transfer using Hive EXPORT_IMPORT.  `hms-mirror` does NOT support incremental updates for transactional/ACID tables. |
\ `-c` `--commit` | na | When running the `-m` (metadata) option with `-ss`/`--shared-storage` this will give you the option to commit ownership to the RIGHT cluster.  Which means that if the table was a managed table in the lower cluster, we'll turn on the `external.table.purge` flag in the table properties. |
| `-dbp` | <dbname_prefix> | If you would like to alter the target database name, usually for testing, this will prepend the database name in the RIGHT cluster |


## Examples

### Replicating 2 database definitions

`hms-mirror -m -db my_first_db,my_second_db`

