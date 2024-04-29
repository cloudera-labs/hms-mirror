# Running

After running the `setup.sh` script, `hms-mirror` will be available in the `$PATH` in a default configuration.

## Assumptions

1. This process will only 'migrate' EXTERNAL and MANAGED (non-ACID/Transactional) table METADATA (not data, except with [SQL](hms-mirror-sql.md) and [EXPORT_IMPORT](hms-mirror-export-import.md) ).
2. MANAGED tables replicated to the **RIGHT** cluster will be converted to "EXTERNAL" tables for the 'metadata' stage.  They will be tagged as 'legacy managed' in the **RIGHT** cluster.  They will be assigned the `external.table.purge=true` flag, to continue the behaviors of the legacy managed tables.
1. The **RIGHT** cluster has 'line of sight' to the **LEFT** cluster.
2. The **RIGHT** cluster has been configured to access the **LEFT** cluster storage. See [link clusters](Linking-Cluster-Storage-Layers.md).  This is the same configuration required to support `distcp` from the **RIGHT** cluster to the **LEFT** cluster.
3. The movement of metadata/data is from the **LEFT** cluster to the **RIGHT** cluster.
4. With Kerberos, each cluster must share the same trust mechanism.
- The **RIGHT** cluster must be Kerberized IF the **LEFT** cluster is.
- The **LEFT** cluster does NOT need to be kerberized if the **RIGHT** cluster is kerberized.
7. The **LEFT* cluster does NOT have access to the **RIGHT** cluster.
8. The credentials use by 'hive' (doas=false) in the **RIGHT** cluster must have access to the required storage (hdfs) locations on the lower cluster.
- If the **RIGHT** cluster is running impersonation (doas=true), that user must have access to the required storage (hdfs) locations on the lower cluster.

### Transfer DATA, beyond the METADATA

HMS-Mirror does NOT migrate data between clusters unless you're using the [SQL](hms-mirror-sql.md) or [EXPORT_IMPORT](hms-mirror-export-import.md) data strategies.  In some cases where data is co-located, you don't need to move it.  IE: Cloud to Cloud.  As long as the new cluster environment has access to the original location.  This is the intended target for strategies [COMMON](hms-mirror-common.md) and to some extend [LINKED](hms-mirror-linked.md).

When you do need to move data, `hms-mirror` creates a workbook of 'source' and 'target' locations in an output file called `distcp_workbook.md`.  Use this to help build a transfer job in `distcp` using the `-f` option to specify multiple sources. 

### Application Return Codes

The `hms-mirror` application returns `0` when everything is ok.  If there is a configuration validation issue, the return code will be a negative value who's absolute value represents the bitSets cumulative `OR` value.  See: [MessageCodes](https://github.com/cloudera-labs/hms-mirror/blob/main/src/main/java/com/cloudera/utils/hadoop/hms/mirror/MessageCode.java) for values and [Messages.java for the calculation](https://github.com/cloudera-labs/hms-mirror/blob/df9df251803d8722ef67426a73cbcfb86f981d3e/src/main/java/com/cloudera/utils/hadoop/hms/mirror/Messages.java#L26).

When you receive an error code (negative value), you'll also get the items printed to the screen and the log that make up that error code.

For example, the following would yield a code of `-2305843009214742528` (20 and 61).

```
******* ERRORS *********
20:STORAGE_MIGRATION requires you to specify PATH location for 'managed' and 'external' tables (-wd, -ewd) to migrate storage.  These will be appended to the -smn (storage-migration-namespace) parameter and used to set the 'database' LOCATION and MANAGEDLOCATION properties
61:You're using the same namespace in STORAGE_MIGRATION, without `-rdl` you'll need to ensure you have `-glm` set to map locations.
```

```((2^20)+(2^61))*-1=-2305843009214742528```


## Running Against a LEGACY (Non-CDP) Kerberized HiveServer2

`hms-mirror` is pre-built with CDP libraries and WILL NOT be compatible with LEGACY kerberos environments. A Kerberos connection can only be made to ONE cluster when the clusters are NOT running the same 'major' version of Hadoop.

To attach to a LEGACY HS2, run `hms-mirror` with the `--hadoop-classpath` command-line option.  This will strip the CDP libraries from `hms-mirror` and use the hosts Hadoop libraries by calling `hadoop classpath` to locate the binaries needed to do this.



## On-Prem to Cloud Migrations

On-Prem to Cloud Migrations should run `hms-mirror` from the LEFT cluster since visibility in this scenario is usually restricted to LEFT->RIGHT.

If the cluster is an older version of Hadoop (HDP 2, CDH 5), your connection to the LEFT HS2 should NOT be kerberized.  Use LDAP or NO_AUTH.

The clusters LEFT hcfsNamespace (clusters:LEFT:hcfsNamespace) should be the LEFT clusters HDFS service endpoint.  The RIGHT hcfsNamespace (clusters:RIGHT:hcfsNamespace) should be the *target* root cloud storage location.  The LEFT clusters configuration (/etc/hadoop/conf) should have all the necessary credentials to access this location.  Ensure that the cloud storage connectors are available in the LEFT environment.

There are different strategies available for migrations between on-prem and cloud environments.

### SCHEMA_ONLY

This is a schema-only transfer, where the `hcfsNamespace` in the metadata definitions is 'replaced' with the `hcfsNamespace` value defined on the RIGHT.  NOTE: The 'relative' directory location is maintained in the migration.

No data will be migrated in this case.

There will be a [`distcp` Planning Workbook](hms-mirror-features.md#distcp-planning-workbook-and-scripts) generated with a plan that can be used to build the data migration process with `distcp`.

### INTERMEDIATE

## Connections

`hms-mirror` connects to 3 endpoints.  The hive jdbc endpoints for each cluster (2) and the `hdfs` environment configured on the running host.  This means you'll need:
- JDBC drivers to match the JDBC endpoints
- For **non** CDP 7.x environments and Kerberos connections, an edge node with the current Hadoop libraries.

See the [config](hms-mirror-cfg.md) section to setup the config file for `hms-mirror`.

### Configuring the Libraries

#### AUX_LIBS - CLASSPATH Additions

##### S3

The directory $HOME/.hms-mirror/aux_libs will be scanned for 'jar' files. Each 'jar' will be added the java classpath of the application. Add any required libraries here.

The application contains all the necessary hdfs classes already. You will need to add to the aux_libs directory the following:

- JDBC driver for HS2 Connectivity (only when using Kerberos)
- AWS S3 Drivers, if s3 is used to store Hive tables. (appropriate versions)
    - hadoop-aws.jar
    - aws-java-sdk-bundle.jar

#### JDBC Connection Strings for HS2

See the [Apache docs](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=30758725#HiveServer2Clients-JDBC) regarding these details if you are using the environment 'Standalone' JDBC drivers.  Other drivers may have different connect string requirements.

The drivers for the various environments are located:

- HDP - `/usr/hdp/current/hive-server2/jdbc/hive-jdbc-<version>-standalone.jar` (NOTE: Use the hive-1 standalone jar file for HDP 2.6.5, not the hive-2 jar)
- CDH/CDP - `/opt/cloudera/parcels/CDH/jars/hive-jdbc-<version>-standalone.jar`

#### Non-Kerberos Connections

The most effortless connections are 'non-kerberos' JDBC connections either to HS2 with AUTH models that aren't **Kerberos** or through a **Knox** proxy.  Under these conditions, only the __standalone__ JDBC drivers are required.  Each of the cluster configurations contains an element `jarFile` to identify those standalone libraries.

```yaml
    hiveServer2:
      uri: "<jdbc-url>"
      connectionProperties:
        user: "*****"
        password: "*****"
      jarFile: "<environment-specific-jdbc-standalone-driver>"
```

When dealing with clusters supporting different Hive (Hive 1 vs. Hive 3) versions, the JDBC drivers aren't forward OR backward compatible between these versions.  Hence, each JDBC jar file is loaded in a sandbox that allows us to use the same driver class, but isolates it between the two JDBC jars.

Place the two jdbc jar files in any directory **EXCEPT** `$HOME/.hms-mirror/aux_libs` and reference the full path in the `jarFile` property for that `hiveServer2` configuration.

_SAMPLE Commandline_

`hms-mirror -db tpcds_bin_partitioned_orc_10`

#### Kerberized Connections

`hms-mirror` relies on the Hadoop libraries to connect via 'kerberos'.  Suppose the clusters are running different versions of Hadoop/Hive. In that case, we can only support connecting to one of the clusters via Kerberos.  While `hms-mirror` is built with the dependencies for Hadoop 3.1 (CDP 7.1.x), we do NOT have embedded all the libraries to establish a connection to kerberos.  Kerberos connections are NOT supported in the 'sandbox' configuration we discussed above.

To connect to a 'kerberized' jdbc endpoint, you need to include `--hadoop-classpath` with the commandline options.  This will load the environments `hadoop classpath` libraries for the application.  To connect with a kerberos endpoint, `hms-mirror` must be run on an edgenode of the platform that is kerberized to ensure we pick up the correct supporting libraries via `hadoop classpath`, AND the jdbc driver for that environment must be in the `$HOME/.hms-mirror/cfg/aux_libs` directory so it is a part of the applications classpath at start up.  DO NOT define that environments `jarFile` configuration property.

There are three scenarios for kerberized connections.

| Scenario | LEFT Kerberized/Version | RIGHT Kerberized/Version | Notes                                                                                                                                                                                                                                                                                                                                                                                                                     | Sample Commandline                                               |
|:---|:---:|:---:|:--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|:-----------------------------------------------------------------|
| 1 | No <br/> HDP2 | Yes <br/> HDP 3 or CDP 7 | <ol><li>'hms-mirror' needs to be run from a node on the HDP3/CDP cluster.</li><li>place the RIGHT cluster jdbc jar file in `$HOME/.hms-mirror/aux_libs` (yes this contradicts some earlier directions)</li><li>comment out the `jarFile` property for the RIGHT cluster hiveServer2 setting.</li></ol>                                                                                                                    | `hms-mirror -db tpcds_bin_partitioned_orc_10 --hadoop-classpath` |
| 2 | YES <br/> HDP 3 or CDP 7 | YES <br/>HDP 3 or CDP 7 | <ol><li>'hms-mirror' needs to be run from a node on the HDP3/CDP cluster.</li><li>place the RIGHT cluster jdbc jar file in $HOME/.hms-mirror/aux_libs (yes this contradicts some earlier directions)</li><li>comment out the `jarFile` property for the LEFT AND RIGHT cluster hiveServer2 settings.</li></ol>                                                                                                            | `hms-mirror -db tpcds_bin_partitioned_orc_10 --hadoop-classpath` |
| 3 | YES<br/>HDP2 or Hive 1 | NO <br/> HDP 3 or CDP 7 | Limited testing, but you'll need to run `hms-mirror` ON the **LEFT** cluster and include the LEFT clusters hive standalone jdbc driver in `$HOME/.hms-mirror/cfg/aux_libs`. | `hms-mirror -db tpcds_bin_partitioned_orc_10 --hadoop-classpath` |
| 4 | YES<br/>HDP2 or Hive 1 | YES <br/> HDP2 or Hive 1 | <ol><li>The Kerberos credentials must be TRUSTED to both clusters</li><li>Add `--hadoop-classpath` as a commandline option to `hms-mirror`.  This replaces the prebuilt Hadoop 3 libraries with the current environments Hadoop Libs.</li><li>Add the jdbc standalone jar file to `$HOME/.hms-mirror/aux_libs`</li><li>Comment out/remove the `jarFile` references for BOTH clusters in the configuration file.</li></ol> | `hms-mirror -db tpcds_bin_partitioned_orc_10 --hadoop-classpath` |

For Kerberos JDBC connections, ensure you are using an appropriate Kerberized Hive URL.

`jdbc:hive2://s03.streever.local:10000/;principal=hive/_HOST@STREEVER.LOCAL`

#### ZooKeeper Discovery Connections

You may run into issues connecting to an older cluster using ZK Discovery.  This mode brings in a LOT of the Hadoop ecosystem classes and may conflict across environments.  We recommend using ZooKeeper discovery on only the RIGHT cluster.  Adjust the LEFT cluster to access HS2 directly.

#### TLS/SSL Connections

If your HS2 connection requires TLS, you will need to include that detail in the jdbc 'uri' you provide.  In addition, if the SSL certificate is 'self-signed' you will need to include details about the certificate to the java environment.  You have 2 options:

- Set the JAVA_OPTS environment with the details about the certificate.
    - `export JAVA_OPTS=-Djavax.net.ssl.trustStore=/home/dstreev/certs/gateway-client-trust.jks -Djavax.net.ssl.trustStorePassword=changeit`
- Add `-D` options to the `hms-mirror` commandline to inject those details.
    - `hms-mirror -db test_db -Djavax.net.ssl.trustStore=/home/dstreev/certs/gateway-client-trust.jks -Djavax.net.ssl.trustStorePassword=changeit`



## Troubleshooting

If each JDBC endpoint is Kerberized and the connection to the LEFT or RIGHT is successful, both NOT both, and the program seems to hang with no exception...  it's most likely that the Kerberos ticket isn't TRUSTED across the two environments.  You will only be able to support a Kerberos connection to the cluster where the ticket is trusted.  The other cluster connection will need to be anything BUT Kerberos.

Add `--show-cp` to the `hms-mirror` command line to see the classpath used to run.

The argument `--hadoop-classpath` allows us to replace the embedded Hadoop Libs (v3.1) with the libs of the current platform via a call to `hadoop classpath`.  This is necessary to connect to kerberized Hadoop v2/Hive v1 environments.

Check the location and references to the JDBC jar files.  General rules for Kerberos Connections:
- The JDBC jar file should be in the `$HOME/.hms-mirror/aux_libs`.  For Kerberos connections, we've seen issues attempting to load this jar in a sandbox, so this makes it available to the global classpath/loader.
- Get a Kerberos ticket for the running user before launching `hms-mirror`.

### "Unrecognized Hadoop major version number: 3.1.1.7.1...0-257"

This happens when you're trying to connect to an HS2 instance.
