# Configuration

## LEFT and RIGHT Clusters

`hms-mirror` defines clusters as LEFT and RIGHT.  The LEFT cluster is the source of the metadata and the RIGHT cluster is the target.  The LEFT cluster is usually the older cluster version.  Regardless, under specific scenario's, `hms-mirror` will use an HDFS client to check directories and move small amounts of data (AVRO schema files).  `hms-mirror` will depend on the configuration of the node it's running on to locate the 'hcfs filesystem'.  This means that the `/etc/hadoop/conf` directory should contain all the environments settings to successfully connect to `hcfs (Hadoop Compatible File System`.

The configuration is done via a 'yaml' file, details below.

There are two ways to get started:
- The first time you run `hms-mirror` and it can't find a configuration, it will walk you through building one and save it to `$HOME/.hms-mirror/cfg/default.yaml`.  Here's what you'll need to complete the setup:
    - URI's for each clusters HiveServer2
    - STANDALONE jar files for EACH Hive version.
        - We support the Apache Hive based drivers for Hive 1 and Hive2/3.
        - Recently added support for the Cloudera JDBC driver for CDP.
    - Username and Password for non-kerberized connections.
        - Note: `hms-mirror` will only support one kerberos connection.  For the other, use another AUTH method.
    - The hcfs (Hadoop Compatible FileSystem) protocol and prefix used for the hive table locations in EACH cluster.
- Use the [template yaml](hms-mirror-Default-Configuration-Template.md) for reference and create a `default.yaml` in the running users `$HOME/.hms-mirror/cfg` directory.

You'll need JDBC driver jar files that are **specific* to the clusters you'll integrate.  If the **LEFT** cluster isn't the same version as the **RIGHT** cluster, don't use the same JDBC jar file, especially when integrating Hive 1 and Hive 3 services.  The Hive 3 driver is NOT backwardly compatible with Hive 1.

See the [running](hms-mirror-running.md) section for examples on running `hms-mirror` for various environment types and connections.

