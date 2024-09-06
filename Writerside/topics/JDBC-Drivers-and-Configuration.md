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

Starting with the Apache Standalone driver shipped with CDP 7.1.8 cummulative hot fix parcels, you will need to include additional jars in the configuration `jarFile` configuration, due to some packaging adjustments.

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

