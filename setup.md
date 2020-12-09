# Setup

## Obtaining HMS-Mirror

### Building

`mvn clean install`

Produces a 'tarball' that can be distributed.  Copy the `target/hms-mirror-<version>-dist.tar.gz` to an edge node on the **UPPER** cluster.

### Obtaining Pre-Built Binary

[![Download the LATEST Binary](./images/download.png)](https://github.com/dstreev/hms-mirror/releases)

## HMS-Mirror Setup from Binary Distribution

On the edgenode:
- Expand the tarball `tar zxvf hms-mirror-<version>-dist.tar.gz`.  
  > This produces a child `hms-mirror` directory.
- As the root user (or `sudo`), run `hms-mirror/setup.sh`.

## Configuration

Use the [template yaml](./configs/default.template.yaml) for reference and create `default.yaml` in running users `$HOME/.hms-mirror/cfg`.

You'll need a jdbc driver jar files that are **specific* to the clusters you'll integrate with.  If the **LOWER** cluster isn't the same version as the **UPPER** cluster, don't use the same jdbc jar file.

**Configuration Comments**

`transferDbPrefix` - The prefix used to create a transfer database in the **LOWER** cluster.

`exportBaseDirPrefix` - The directory where the hive __EXPORTS__ will go in reference the to **LOWER* cluster.  If the directory doesn't start with a __protocol__, the `fs.defaultFS` will be used.  If you are working with cloud storage, ensure you use the applicable __protocol__ and __bucket_.

`overwriteTable` - While __IMPORTING__ the tables in the **UPPER* cluster, drop the existing table (if exists), before the __IMPORT__.

`parallelism` - Set the number of threads that will be allocated to work through the conversion process.  The higher this number, the more the load will be on the target 'metastore'.

`clusters:LOWER` - The source cluster for the migration.

`clusters:UPPER` - The target cluster for the migration.

`clusters:LOWER|UPPER:legacyHive` - Identifies the cluster as a Hive 1/2 cluster.  Used to determine 'translation' rules from 'Managed' to 'External/Purge' tables.

`clusters:LOWER|UPPER:hcfsNamespace` - The Hadoop Compatible File System protocol and namespace. IE: `hdfs://PROD` or `s3a://my_bucket`.

`clusters:LOWER|UPPER:partitionDiscovery:auto` - 

`clusters:LOWER|UPPER:partitionDiscovery:initMSCK` - After the __IMPORT__ into the **UPPER** cluster, run `MSCK REPAIR TABLE` to discover all the partitions.

`clusters:LOWER|UPPER:hiveServer2:jarFile` - The local location of the clusters jdbc driver.

`clusters:LOWER|UPPER:hiveServer2:uri` - The jdbc uri used to connect to the target Hive Server 2.

`clusters:LOWER|UPPER:hiveServer2:connectionProperties:user` - Username for auth

`clusters:LOWER|UPPER:hiveServer2:connectionProperties:password` - Password for auth

