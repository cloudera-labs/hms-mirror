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

## Running `hms-mirror`

`hms-mirror` connects to 3 endpoints.  The hive jdbc endpoints for each cluster (2) and the `hdfs` environment configured on the running host.  Which means you'll need:
- jdbc drivers to match the jdbc endpoints
- For **non** CDP 7.x environments and Kerberos connections, an edge node with the current hadoop libraries.

See the [config](./configuration.md) section to setup the config file for `hms-mirror`.

### Configuring the Libraries

#### Non-Kerberos Connections

The easiest connections are 'non-kerberos' jdbc connections, either to HS2 with AUTH models that aren't **Kerberos** or through a **Knox** proxy.  Under these conditions, only the __standalone__ jdbc drivers are required.  Each of the cluster configurations contains an element `jarFile` to identify those standalone libraries.

```yaml
    hiveServer2:
      uri: "<jdbc-url>"
      connectionProperties:
        user: "*****"
        password: "*****"
      jarFile: "<environment-specific-jdbc-standalone-driver>"
```

When dealing with clusters supporting different version of Hive (Hive 1 vs. Hive 3), the jdbc drivers aren't forward OR backward compatible between these versions.  Hence, each jdbc jar file is loaded in a sandbox that allows us to use the same driver class, but isolate it between the two jdbc jars.

Place the two jdbc jar files in any directory **EXCEPT** `$HOME/.hms-mirror/aux_libs` and reference the full path in the `jarFile` property for that `hiveServer2` configuration.

_SAMPLE Commandline_

`hms-mirror -db tpcds_bin_partitioned_orc_10`

#### Kerberized Connections

`hms-mirror` relies on the Hadoop libraries to connect via 'kerberos'.  If the clusters are running different versions of Hadoop/Hive, we can only support connecting to one of the clusters via kerberos.  `hms-mirror` is built with the dependencies for Hadoop 3.1 (CDP 7.1.x).  Kerberos connections are NOT supported in the 'sandbox' configuration we discussed above.

There are three scenarios for kerberized connections.

| Scenario | LEFT Kerberized/Version | RIGHT Kerberized/Version | Notes | Sample Commandline |
|:---|:---:|:---:|:---|:---|
| 1 | No <br/> HDP2 | Yes <br/> HDP 3 or CDP 7 | <ol><li>The hadoop libs are built into `hms-mirror` for this scenario.</li><li>'hms-mirror' needs to be run from a node on the HDP3/CDP cluster.</li><li>place the RIGHT cluster jdbc jar file in `$HOME/.hms-mirror/aux_libs` (yes this contradicts some earlier directions)</li><li>comment out the `jarFile` property for the RIGHT cluster hiveServer2 setting.</li></ol> | `hms-mirror -db tpcds_bin_partitioned_orc_10` |
| 2 | YES <br/> HDP 3 or CDP 7 | YES <br/>HDP 3 or CDP 7 | <ol><li>The hadoop libs are built into `hms-mirror` for this scenario.</li><li>'hms-mirror' needs to be run from a node on the HDP3/CDP cluster.</li><li>place the RIGHT cluster jdbc jar file in $HOME/.hms-mirror/aux_libs (yes this contradicts some earlier directions)</li><li>comment out the `jarFile` property for the LEFT AND RIGHT cluster hiveServer2 settings.</li></ol> | `hms-mirror -db tpcds_bin_partitioned_orc_10` |
| 3 | YES<br/>HDP2 or Hive 1 | NO <br/> HDP 3 or CDP 7 | Not Supported when `hms-mirror` run from the RIGHT cluster. | `hms-mirror -db tpcds_bin_partitioned_orc_10` |
| 4 | YES<br/>HDP2 or Hive 1 | YES <br/> HDP2 or Hive 1 | <ol><li>The Kerberos credentials must be TRUSTED to both clusters</li><li>Add `--hadoop-classpath` as a commandline option to `hms-mirror`.  This replaces the prebuilt Hadoop 3 libraries with the current environments Hadoop Libs.</li><li>Add the jdbc standalone jar file to `$HOME/.hms-mirror/aux_libs`</li><li>Comment out/remove the `jarFile` references for BOTH clusters in the configuration file.</li></ol> | `hms-mirror -db tpcds_bin_partitioned_orc_10 --hadoop-classpath` |

For kerberos jdbc connections, ensure you are using an appropriate Kerberized Hive URL.

`jdbc:hive2://s03.streever.local:10000/;principal=hive/_HOST@STREEVER.LOCAL`

### Troubleshooting

If each JDBC endpoint is Kerberized and the connection to the LEFT or RIGHT is succesful, both NOT both and the program seems to just hang with no exception...  it's most likely that the Kerberos ticket isn't TRUSTED across the two environments.  You will only be able to support a kerberos connection to the cluster where the ticket is trusted.  The other cluster connection will need to be anything BUT kerberos. 

Add `--show-cp` to the `hms-mirror` commandline to see the classpath used to run.

The argument `--hadoop-classpath` allows us to replace the embedded Hadoop Libs (v3.1) with the libs of the current platform via a call to `hadoop classpath`.  This is necessary to connect to kerberized Hadoop v2/Hive v1 environments.

Check the location and references to the JDBC jar files.  General rules for Kerberos Connections:
- The jdbc jar file should be in the `$HOME/.hms-mirror/aux_libs`.  For kerberos connections, we've seen issues attempting to load this jar in a sandbox, so this makes it available to the global classpath/loader.
- Get a kerberos ticket for the running user before launching `hms-mirror`.

