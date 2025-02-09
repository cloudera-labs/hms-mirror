# Hive JDBC Drivers and Configuration

`hms-mirror` requires JDBC drivers to connect to the various end-points needed to perform tasks.  The `LEFT` and `RIGHT` cluster endpoints for HiveServer2 require the standalone JDBC drivers that are specific to that Hive version.

`hms-mirror` supports the Apache Hive packaged **standalone** drivers that are found with your distribution.  You can find a copy of this driver in: 

| Platform | Driver Location/Pattern                                                              |
|----------|--------------------------------------------------------------------------------------|
| HDP      | `/usr/hdp/current/hive-client/jdbc/hive-jdbc-<hive-platform-version>-standalone.jar` |
| CDP      | `/opt/cloudera/parcels/CDH/jars/hive-jdbc-<hive-platform-version>-standalone.jar`    |
|          |                                                                                      |


For CDP, we also support to Cloudera JDBC driver found and maintained at on the [Cloudera Hive JDBC Downloads Page](https://www.cloudera.com/downloads/connectors/hive/jdbc).  Note that the URL configurations between the Apache and Cloudera JDBC drivers are different.

Hive JDBC Drivers need to be inline with the version of HS2 you're connecting to.  If the cluster is an HDP cluster, get the appropriate **standalone** driver from that cluster.  These drivers(jar files) should be stored locally on the machine running `hms-mirror` and referenced in the configuration file. 

<warning>
Do NOT put these drivers in ${HOME}/.hms-mirror/aux_libs or any sub-directory of that location.  `hms-mirror` connects to different versions of Hive and the drivers need to be specific to the version of Hive you're connecting to.  To do this, we need to manage the classpath and the drivers in a more controlled manner.  They should NOT be in the applications main classpath
which includes jar files in `$HOME/.hms-mirror/aux_libs`, this will cause connectivity issues.
</warning>

<tabs>
<tab title="Web UI">
<b> Hive Server 2 Configuration</b>

![hs2_cfg.png](hs2_cfg.png)

</tab>
<tab id="cli-hs2" title="CLI">

```yaml
hiveServer2:
  uri: "<cloudera_jdbc_url>"
  driverClassName: "com.cloudera.hive.jdbc.HS2Driver"
  connectionProperties:
    user: "xxx"
    password: "xxx"
```
</tab>
</tabs>

Starting with the Apache Standalone driver shipped with **CDP 7.1.8 cummulative** hot fix parcels, you will need to include additional jars in the configuration `jarFile` configuration, due to some packaging adjustments.

For example: `jarFile: "<cdp_parcel_jars>/hive-jdbc-3.1.3000.7.1.8.28-1-standalone.jar:<cdp_parcel_jars>/log4j-1.2-api-2.18.0.jar:<cdp_parcel_jars>/log4j-api-2.18.0.jar:<cdp_parcel_jars>/log4j-core-2.18.0.jar"` NOTE: The jar file with the Hive Driver MUST be the first in the list of jar files.

The Cloudera JDBC driver shouldn't require additional jars.

## Kerberized HS2 Connections

We currently have validated **kerberos** HS2 connections to CDP clusters using the Hive JDBC driver you'll find in your target CDP distribution.

<warning>
Connections to Kerberized HS2 endpoints on NON-CDP clusters is NOT currently supported.  You will need to use KNOX in HDP to connect to a kerberized HS2 endpoint. For CDH, you can setup a non-kerberized HS2 endpoint to support the migration.
</warning>  

<note>
This process has CHANGED compared to v1.x of `hms-mirror`.  Please adjust your configurations accordingly.
</note>

We NO LONGER need to have the hive JDBC driver in the `aux_libs` directory ($HOME/.hms-mirror/aux_libs).  The driver should be stored locally on the machine running `hms-mirror` and referenced in the configuration file via the `jarFile' attribute.  Follow the same procedure as above for **Kerberized** connections as is done for non-kerberized connections.

![hs2_kerb_config.png](hs2_kerb_config.png)

At this point, just like the previous version of `hms-mirror`, you'll need to have a valid kerberos ticket on the machine running `hms-mirror`.  This is required to authenticate to the kerberized HS2 endpoint.

REMOVE all 'hive' related JDBC jar files from `aux_libs`.  Leaving them there WILL cause conflicts during the service startup.

## Validating HS2 Connectivity

Once you have everything configured, you can validate all connections required by `hms-mirror` through the 
'CONNECTIONS --> Validate' left menu option in the UI.  This will test the connectivity to the various endpoints 
required by `hms-mirror`.

## HDP 3 Connections

The JDBC driver for HDP Hive 3 has some embedded classes for `log4j` that conflict with the `log4j` classes in the `hms-mirror` application.  To resolve this, you can use the Cloudera Apache JDBC driver for HDP 3 Hive.  This driver is compatible with HDP 3 and does not have the `log4j` conflict.

## Hive JDBC Driver Connection Pool Settings

Hive 1/2, which includes HDP 2.x and CDH 5.x / 6.x environments will use the Apache Commons DBCP2 Connection Pool libraries. Hive 3/4, which includes HDP 3.x and CDH 7.x environments will use the HikariCP Connection Pool libraries.

There are a few settings that you can adjust in the `hms-mirror` configuration file to tune the connection pool settings.

For the DBCP2 connection pool, you can set the `maxWaitMillis` setting, which has a default of `5000` milliseconds.

For the HikariCP connection pool, the following connection properties can be adjusted:
`connectionTimeout` (default: `60000` milliseconds)
`validationTimeout` (default: `30000` milliseconds)
`initializationFailTimeout` (default: `10000` milliseconds)

The default setting have been pretty successful in our testing, but you can adjust these settings to meet your needs.

<tabs>
<tab title="CLI">

The DBCP2 connection pool settings can be adjustment through the CLI via the `-pt|--pass-through` option using one of more of the following setting:
- `dbcp2.maxWaitMillis`

EG: `-pt dbcp2.maxWaitMillis=10000`

The Hikari connection pool settings can be adjusted through the CLI via the `-pt|--pass-through` option using one of more of the following setting:
- `hikari.connectionTimeout`
- `hikari.validationTimeout`
- `hikari.initializationFailTimeout`

<note>You can have multiple `-pt` options on the command line</note>

EG: `-pt hikari.connectionTimeout=60000 -pt hikari.validationTimeout=30000 -pt hikari.initializationFailTimeout=10000`

</tab>
<tab title="Web UI">

![hs2_cp_settings.png](hs2_cp_settings.png)![filter_databases.png](filter_databases.png)

</tab>
<tab title="Config File">

```yaml
clusters:
  LEFT:
    environment: "LEFT|RIGHT"
    hiveServer2:
      uri: "..."
      connectionProperties:
        maxWaitMillis: "5000" # DBCP2 connection pool setting
        hikari.validationTimeout: "30000"
        hikari.initializationFailTimeout: "10000"
        hikari.connectionTimeout: "60000"
```

</tab>
</tabs>
