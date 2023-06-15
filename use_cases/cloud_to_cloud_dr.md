# Cloud to Cloud DR (AWS)

We'll cover how to manage a DR scenario where the source cluster is in the cloud and the target cluster is also in the cloud.  The main elements to consider are:
- Hive Metadata (Tables, Databases, Views)
- Data on S3

## Requirements

- The source and target clusters on AWS are running CDP Cloud with an available HS2 endpoint.
- Provide a mechanism to migrate Hive metadata from the source cluster to the target cluster, to include making adjustments to the metadata to account for differences in the clusters storage locations.
- Establish the RPO and RTO for the DR scenario and ensure the migration process can meet those requirements.
- We'll only target **external** tables for this scenario.  Managed tables will require additional considerations, since the data and metadata are intermingled and can't be supported through a simple copy operation.

## Assumptions

- The data on S3 is already replicated to the target cluster through some other mechanism (e.g. S3 replication, etc.).
- The S3 replication will meet the RPO and RTO requirements.
- The data replication is in place before DR is invoked and the scripts are run to build the schemas on the target cluster.
- There are no managed tables being migrated.  I would recommend setting the database property `‘EXTERNAL_TABLES_ONLY’=’TRUE’` with: `ALTER DATABASE <db_name> SET TBLPROPERTIES ('EXTERNAL_TABLES_ONLY'='TRUE');` to ensure only external tables can be created.
- Partitions follow standard naming conventions regarding directory names/structures.  Tables with non-standard partitioning will require additional considerations.  `hms-mirror` doesn't translate partition details and relies on `MSCK REPAIR <table> SYNC PARTITIONS` to discover / rebuild a tables partitions.  If the partitions are not in a standard format, the `MSCK REPAIR` will not work and the partitions will need to be manually created.
- We don't support schema evolution.  All tables will be created in there current state.
- The "LEFT" clusters `hcfsNamespace` can only address a single namespace at a time.  If you have multiple namespaces, you'll need to run `hms-mirror` multiple times, once for each namespace.  The "RIGHT" cluster can address multiple namespaces through the `hcfsNamespace` element.  This element is used to match and adjust the storage location of the tables on the target cluster.

## The Process

The process is fairly straight forward.  We'll use `hms-mirror` to migrate the Hive metadata from the source cluster to the target cluster.  We'll use the `--common-storage` or set the `hcfsNamespace` element for the RIGHT cluster to ensure the schemas are built with the DR bucket adjustments.

You have a few options regarding the transfer:
- If the target is truly a DR cluster, you can run `hms-mirror` on the source cluster and generate the metadata files locally.  Then copy the metadata files to the target cluster and build out the schemas there.  This doesn't need to be done until the DR is invoked.
- If you want/need to keep the metadata in-sync between the clusters, you can run `hms-mirror` with the `-ro` and `-sync` flags (and eventually with `-e`) to keep the metadata in-sync between the clusters.  Tables created on the source cluster will require the data to be replicated to the target cluster before the table can be created in DR.  While we're only migrating *external* tables, they may have set `external.table.purge` to `true` on the source cluster.  In this case, these tables will be set to **NON** purge on the target cluster. This is to prevent the table data (being managed through S3 replication) from being dropped by subsequent sync runs where the tables might have changed.   

## Running `hms-mirror`

### Configuration

This file should be named `$HOME/.hms-mirror/cfg/default.yaml`

```yaml
clusters:
  LEFT:
    environment: "LEFT"
    legacyHive: false
    hcfsNamespace: "s3a://<my_source_s3_bucket>"
    hiveServer2:
      # Recommend using a KNOX endpoint to remove need for Kerberos Authentication
      uri: "jdbc:hive2://<my_source_hs2_endpoint>"
      connectionProperties:
        user: "<user>"
        maxWaitMillis: "5000"
        password: "*****"
        maxTotal: "-1"
      jarFile: "<local_location_of_hive-jdbc_driver>"
  RIGHT:
    environment: "RIGHT"
    legacyHive: false
    hcfsNamespace: "s3a://<my_target_s3_bucket>"
    hiveServer2:
      uri: "jdbc:hive2://<my_target_hs2_endpoint>"
      connectionProperties:
        user: "<user>"
        maxWaitMillis: "5000"
        password: "*****"
        maxTotal: "-1"
      jarFile: "<local_location_of_hive-jdbc_driver>"
    partitionDiscovery:
      # Optional, but recommended it the cluster isn't overburdened.
      auto: true
      # Required if auto is false and/or you want to ensure the partitions are in sync after the 
      # transfer is made.
      initMSCK: true
```

### Command Lines

`hms-mirror --hadoop-classpath -d SCHEMA_ONLY -db <db_comma_separated_list> -ro -sync`

