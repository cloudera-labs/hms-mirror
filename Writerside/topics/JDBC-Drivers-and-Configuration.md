# JDBC Drivers and Configuration

`hms-mirror` requires JDBC drivers to connect to the various end-points needed to perform it's tasks.  The `LEFT` and `RIGHT` cluster endpoints for HiveServer2 require the standalone JDBC drivers that are specific to that Hive version.

`hms-mirror` supports the Apache and packaged hive **standalone** drivers that are found with your distribution.  For CDP, we also support to Cloudera JDBC driver found and maintained at on the [Cloudera Hive JDBC Downloads Page](https://www.cloudera.com/downloads/connectors/hive/jdbc).  Note that the URL configurations between the Apache and Cloudera JDBC drivers are different.

When using the Cloudera JDBC driver, you'll need to add the property `driverClassName: "com.cloudera.hive.jdbc.HS2Driver"` to the `hiveServer2` configuration. If you're NOT using the Cloudera JDBC driver, just remove the property.

```yaml
hiveServer2:
  uri: "<cloudera_jdbc_url>"
  driverClassName: "com.cloudera.hive.jdbc.HS2Driver"
  connectionProperties:
    user: "xxx"
    password: "xxx"
```

Starting with the Apache Standalone driver shipped with CDP 7.1.8 cummulative hot fix parcels, you will need to include additional jars in the configuration `jarFile` configuration, due to some packaging adjustments.

For example: `jarFile: "<cdp_parcel_jars>/hive-jdbc-3.1.3000.7.1.8.28-1-standalone.jar:<cdp_parcel_jars>/log4j-1.2-api-2.18.0.jar:<cdp_parcel_jars>/log4j-api-2.18.0.jar:<cdp_parcel_jars>/log4j-core-2.18.0.jar"` NOTE: The jar file with the Hive Driver MUST be the first in the list of jar files.

The Cloudera JDBC driver shouldn't require additional jars.

