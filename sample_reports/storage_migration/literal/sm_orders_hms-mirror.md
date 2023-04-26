# HMS-Mirror for: sm_orders

v.1.5.4.6-SNAPSHOT
---
## Run Log

| Date | Elapsed Time |
|:---|:---|
| 2023-04-26 12:07:02 | 1.86 secs |

## Config:
```
---
acceptance:
  silentOverride: null
  backedUpHDFS: null
  backedUpMetastore: null
  trashConfigured: null
  potentialDataLoss: null
clusters:
  LEFT:
    environment: "LEFT"
    legacyHive: false
    hcfsNamespace: "hdfs://HOME90"
    hiveServer2:
      uri: "jdbc:hive2://s03.streever.local:8443/;ssl=true;transportMode=http;httpPath=gateway/cdp-proxy-api/hive;sslTrustStore=/home/dstreev/bin/certs/gateway-client-trust.jks;trustStorePassword=changeit"
      disconnected: false
      connectionProperties:
        validationQuery: "SELECT 1"
        maxIdle: "2"
        user: "*****"
        maxTotal: "4"
        validationQueryTimeout: "5"
        testOnCreate: "true"
        maxWaitMillis: "5000"
        password: "*****"
        initialSize: "4"
      jarFile: "/home/dstreev/.hms-mirror/aux_libs/ext/hive-jdbc-cdp-standalone.jar"
    partitionDiscovery:
      auto: true
      initMSCK: true
  RIGHT:
    environment: "RIGHT"
    legacyHive: true
    hcfsNamespace: null
    hiveServer2: null
    partitionDiscovery:
      auto: true
      initMSCK: true
commandLineOptions: "[-d, STORAGE_MIGRATION, -db, sm_orders, -ma, -smn, ofs://OHOME90,\
  \ -ewd, /warehouse/tablespace/external/hive, -wd, /warehouse/tablespace/managed/hive,\
  \ -o, conversion/literal]"
copyAvroSchemaUrls: false
dataStrategy: "STORAGE_MIGRATION"
databaseOnly: false
skipLinkCheck: false
databases:
- "sm_orders"
legacyTranslations:
  rowSerde:
    '''org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe''': "'org.apache.hadoop.hive.serde2.MultiDelimitSerDe'"
    '''org.apache.hadoop.hive.contrib.serde2.RegexSerDe''': "'org.apache.hadoop.hive.serde2.RegexSerDe'"
dbPrefix: null
dbRename: null
dumpSource: null
execute: false
flip: false
hybrid:
  exportImportPartitionLimit: 100
  sqlPartitionLimit: 500
  sqlSizeLimit: 1073741824
migrateACID:
  "on": true
  only: false
  artificialBucketThreshold: 2
  partitionLimit: 500
  downgrade: false
  inplace: false
migrateVIEW:
  "on": false
migratedNonNative: false
optimization:
  sortDynamicPartitionInserts: false
  skip: false
  overrides:
    left: {}
    right: {}
  buildShadowStatistics: false
readOnly: false
noPurge: false
replace: false
resetRight: false
resetToDefaultLocation: false
skipFeatures: false
skipLegacyTranslation: false
sqlOutput: true
sync: false
tblRegEx: null
tblExcludeRegEx: null
transfer:
  concurrency: 4
  transferPrefix: "hms_mirror_transfer_"
  shadowPrefix: "hms_mirror_shadow_"
  exportBaseDirPrefix: "/apps/hive/warehouse/export_"
  remoteWorkingDirectory: "hms_mirror_working"
  intermediateStorage: null
  commonStorage: "ofs://OHOME90"
  storageMigration:
    strategy: "SQL"
    distcp: false
    dataFlow: "PULL"
  warehouse:
    managedDirectory: "/warehouse/tablespace/managed/hive"
    externalDirectory: "/warehouse/tablespace/external/hive"
transferOwnership: false

```

### Config Warnings:
- 54:To get the `distcp` workplans add `-dc|--distcp` to commandline.

## Database SQL Statement(s)

### LEFT

```
-- Alter Database Location
ALTER DATABASE sm_orders SET LOCATION "ofs://OHOME90/warehouse/tablespace/external/hive/sm_orders.db"
-- Alter Database Managed Location
ALTER DATABASE sm_orders SET MANAGEDLOCATION "ofs://OHOME90/warehouse/tablespace/managed/hive/sm_orders.db"
```

## DB Issues

### LEFT

* This process, when 'executed' will leave the original tables intact in there renamed version.  They are NOT automatically cleaned up.  Run the produced 'sm_orders_LEFT_CleanUp_execute.sql' file to permanently remove them.  Managed and External/Purge table data will be removed when dropping these tables.  External non-purge table data will remain in storage.

## Table Status (9)

*NOTE* SQL in this report may be altered by the renderer.  Do NOT COPY/PASTE from this report.  Use the LEFT|RIGHT_execution.sql files for accurate scripts

<table>
<tr>
<th style="test-align:left">Table</th>
<th style="test-align:left">Strategy</th>
<th style="test-align:left">Source<br/>Managed</th>
<th style="test-align:left">Source<br/>ACID</th>
<th style="test-align:left">Phase<br/>State</th>
<th style="test-align:right">Duration</th>
<th style="test-align:right">Partition<br/>Count</th>
<th style="test-align:left">Steps</th>
<th style="test-align:left">Added<br/>Properties</th>
<th style="test-align:left">Issues</th>
<th style="test-align:left">SQL</th>
</tr>
<tr>
<td>mngd_order_item_orc</td>
<td>STORAGE_MIGRATION</td>
<td>X</td>
<td>
X</td>
<td>SUCCESS</td>
<td>.08</td>
<td> </td>
<td>
<table>
<tr>
<td>.00</td><td>init</td><td></td></tr>
<tr>
<td>.25</td><td>LEFT</td><td>Fetched Schema</td></tr>
<tr>
<td>.00</td><td>TRANSACTIONAL</td><td>true</td></tr>
<tr>
<td>1.08</td><td>TRANSFER</td><td>STORAGE_MIGRATION</td></tr>
<tr>
<td>.06</td><td>LEFT</td><td>Sql Run Complete for: Selecting DB</td></tr>
<tr>
<td>.00</td><td>LEFT</td><td>Sql Run Complete for: Rename table</td></tr>
<tr>
<td>.01</td><td>LEFT</td><td>Sql Run Complete for: Creating Table</td></tr>
<tr>
<td>.00</td><td>LEFT</td><td>Sql Run Complete for: Moving data to new Namespace</td></tr>
</table>
</td>
<td>
<table><tr>
<th colspan="2">RIGHT</th>
</tr>
<tr>
<td>hmsMirror_Metadata_Stage1</td>
<td>2023-04-26 12:07:02</td>
</tr>
</table></td>
<td>
<table><tr>
<th>RIGHT</th>
</tr>
<tr>
<td>Location Stripped from ACID definition.  Location element in 'CREATE' not allowed in Hive3+</td>
</tr>
</table></td>
<td>
<table><tr>
<th colspan="2">LEFT</th>
</tr>
<tr>
<td>Selecting DB</td>
<td>USE sm_orders</td>
</tr>
<tr>
<td>Rename table</td>
<td> ALTER TABLE mngd_order_item_orc RENAME TO mngd_order_item_orc_b723de0a4008437095141f39c24349ec</td>
</tr>
<tr>
<td>Creating Table</td>
<td>CREATE TABLE `mngd_order_item_orc`(
`order_id` string,
`order_item_id` string,
`product_id` string,
`quantity` bigint,
`cost` double)
ROW FORMAT SERDE
'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
TBLPROPERTIES (
'hmsMirror_Metadata_Stage1'='2023-04-26 12:07:02',
'bucketing_version'='2',
'transactional'='true',
'transactional_properties'='insert_only'
)</td>
</tr>
<tr>
<td>Moving data to new Namespace</td>
<td>FROM mngd_order_item_orc_b723de0a4008437095141f39c24349ec INSERT OVERWRITE TABLE mngd_order_item_orc SELECT *</td>
</tr>
<tr>
<td>Drop table</td>
<td>DROP TABLE IF EXISTS mngd_order_item_orc_b723de0a4008437095141f39c24349ec</td>
</tr>
</table></td>
</tr>
<tr>
<td>mngd_order_item_small_orc</td>
<td>STORAGE_MIGRATION</td>
<td>X</td>
<td>
X</td>
<td>SUCCESS</td>
<td>.06</td>
<td> </td>
<td>
<table>
<tr>
<td>.00</td><td>init</td><td></td></tr>
<tr>
<td>.62</td><td>LEFT</td><td>Fetched Schema</td></tr>
<tr>
<td>.00</td><td>TRANSACTIONAL</td><td>true</td></tr>
<tr>
<td>.71</td><td>TRANSFER</td><td>STORAGE_MIGRATION</td></tr>
<tr>
<td>.06</td><td>LEFT</td><td>Sql Run Complete for: Selecting DB</td></tr>
<tr>
<td>.00</td><td>LEFT</td><td>Sql Run Complete for: Rename table</td></tr>
<tr>
<td>.00</td><td>LEFT</td><td>Sql Run Complete for: Creating Table</td></tr>
<tr>
<td>.00</td><td>LEFT</td><td>Sql Run Complete for: Moving data to new Namespace</td></tr>
</table>
</td>
<td>
<table><tr>
<th colspan="2">RIGHT</th>
</tr>
<tr>
<td>hmsMirror_Metadata_Stage1</td>
<td>2023-04-26 12:07:02</td>
</tr>
</table></td>
<td>
<table><tr>
<th>RIGHT</th>
</tr>
<tr>
<td>Location Stripped from ACID definition.  Location element in 'CREATE' not allowed in Hive3+</td>
</tr>
</table></td>
<td>
<table><tr>
<th colspan="2">LEFT</th>
</tr>
<tr>
<td>Selecting DB</td>
<td>USE sm_orders</td>
</tr>
<tr>
<td>Rename table</td>
<td> ALTER TABLE mngd_order_item_small_orc RENAME TO mngd_order_item_small_orc_8108bc20c7ef4fc59611d25459fecf63</td>
</tr>
<tr>
<td>Creating Table</td>
<td>CREATE TABLE `mngd_order_item_small_orc`(
`order_id` string,
`order_item_id` string,
`product_id` string,
`quantity` bigint,
`cost` double)
ROW FORMAT SERDE
'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
TBLPROPERTIES (
'hmsMirror_Metadata_Stage1'='2023-04-26 12:07:02',
'bucketing_version'='2',
'transactional'='true',
'transactional_properties'='insert_only'
)</td>
</tr>
<tr>
<td>Moving data to new Namespace</td>
<td>FROM mngd_order_item_small_orc_8108bc20c7ef4fc59611d25459fecf63 INSERT OVERWRITE TABLE mngd_order_item_small_orc SELECT *</td>
</tr>
<tr>
<td>Drop table</td>
<td>DROP TABLE IF EXISTS mngd_order_item_small_orc_8108bc20c7ef4fc59611d25459fecf63</td>
</tr>
</table></td>
</tr>
<tr>
<td>mngd_order_orc</td>
<td>STORAGE_MIGRATION</td>
<td>X</td>
<td>
X</td>
<td>SUCCESS</td>
<td>.06</td>
<td> </td>
<td>
<table>
<tr>
<td>.00</td><td>init</td><td></td></tr>
<tr>
<td>.97</td><td>LEFT</td><td>Fetched Schema</td></tr>
<tr>
<td>.00</td><td>TRANSACTIONAL</td><td>true</td></tr>
<tr>
<td>.36</td><td>TRANSFER</td><td>STORAGE_MIGRATION</td></tr>
<tr>
<td>.06</td><td>LEFT</td><td>Sql Run Complete for: Selecting DB</td></tr>
<tr>
<td>.00</td><td>LEFT</td><td>Sql Run Complete for: Rename table</td></tr>
<tr>
<td>.00</td><td>LEFT</td><td>Sql Run Complete for: Creating Table</td></tr>
<tr>
<td>.00</td><td>LEFT</td><td>Sql Run Complete for: Moving data to new Namespace</td></tr>
</table>
</td>
<td>
<table><tr>
<th colspan="2">RIGHT</th>
</tr>
<tr>
<td>hmsMirror_Metadata_Stage1</td>
<td>2023-04-26 12:07:02</td>
</tr>
</table></td>
<td>
<table><tr>
<th>RIGHT</th>
</tr>
<tr>
<td>Location Stripped from ACID definition.  Location element in 'CREATE' not allowed in Hive3+</td>
</tr>
</table></td>
<td>
<table><tr>
<th colspan="2">LEFT</th>
</tr>
<tr>
<td>Selecting DB</td>
<td>USE sm_orders</td>
</tr>
<tr>
<td>Rename table</td>
<td> ALTER TABLE mngd_order_orc RENAME TO mngd_order_orc_8e63f5ebb9394e12a77f4e3f62eb3b52</td>
</tr>
<tr>
<td>Creating Table</td>
<td>CREATE TABLE `mngd_order_orc`(
`id` string,
`user_id` string,
`order_date` date,
`status` string)
ROW FORMAT SERDE
'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
TBLPROPERTIES (
'hmsMirror_Metadata_Stage1'='2023-04-26 12:07:02',
'bucketing_version'='2',
'transactional'='true',
'transactional_properties'='insert_only'
)</td>
</tr>
<tr>
<td>Moving data to new Namespace</td>
<td>FROM mngd_order_orc_8e63f5ebb9394e12a77f4e3f62eb3b52 INSERT OVERWRITE TABLE mngd_order_orc SELECT *</td>
</tr>
<tr>
<td>Drop table</td>
<td>DROP TABLE IF EXISTS mngd_order_orc_8e63f5ebb9394e12a77f4e3f62eb3b52</td>
</tr>
</table></td>
</tr>
<tr>
<td>mngd_order_small_orc</td>
<td>STORAGE_MIGRATION</td>
<td>X</td>
<td>
X</td>
<td>SUCCESS</td>
<td>.02</td>
<td> </td>
<td>
<table>
<tr>
<td>.00</td><td>init</td><td></td></tr>
<tr>
<td>.96</td><td>LEFT</td><td>Fetched Schema</td></tr>
<tr>
<td>.00</td><td>TRANSACTIONAL</td><td>true</td></tr>
<tr>
<td>.39</td><td>TRANSFER</td><td>STORAGE_MIGRATION</td></tr>
<tr>
<td>.02</td><td>LEFT</td><td>Sql Run Complete for: Selecting DB</td></tr>
<tr>
<td>.00</td><td>LEFT</td><td>Sql Run Complete for: Rename table</td></tr>
<tr>
<td>.00</td><td>LEFT</td><td>Sql Run Complete for: Creating Table</td></tr>
<tr>
<td>.00</td><td>LEFT</td><td>Sql Run Complete for: Moving data to new Namespace</td></tr>
</table>
</td>
<td>
<table><tr>
<th colspan="2">RIGHT</th>
</tr>
<tr>
<td>hmsMirror_Metadata_Stage1</td>
<td>2023-04-26 12:07:02</td>
</tr>
</table></td>
<td>
<table><tr>
<th>RIGHT</th>
</tr>
<tr>
<td>Location Stripped from ACID definition.  Location element in 'CREATE' not allowed in Hive3+</td>
</tr>
</table></td>
<td>
<table><tr>
<th colspan="2">LEFT</th>
</tr>
<tr>
<td>Selecting DB</td>
<td>USE sm_orders</td>
</tr>
<tr>
<td>Rename table</td>
<td> ALTER TABLE mngd_order_small_orc RENAME TO mngd_order_small_orc_be1610dd2d4a43128d9b1adf12cddce8</td>
</tr>
<tr>
<td>Creating Table</td>
<td>CREATE TABLE `mngd_order_small_orc`(
`id` string,
`user_id` string,
`order_date` date,
`status` string)
ROW FORMAT SERDE
'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
TBLPROPERTIES (
'hmsMirror_Metadata_Stage1'='2023-04-26 12:07:02',
'bucketing_version'='2',
'transactional'='true',
'transactional_properties'='insert_only'
)</td>
</tr>
<tr>
<td>Moving data to new Namespace</td>
<td>FROM mngd_order_small_orc_be1610dd2d4a43128d9b1adf12cddce8 INSERT OVERWRITE TABLE mngd_order_small_orc SELECT *</td>
</tr>
<tr>
<td>Drop table</td>
<td>DROP TABLE IF EXISTS mngd_order_small_orc_be1610dd2d4a43128d9b1adf12cddce8</td>
</tr>
</table></td>
</tr>
<tr>
<td>order_item_orc</td>
<td>STORAGE_MIGRATION</td>
<td></td>
<td>
</td>
<td>SUCCESS</td>
<td>.08</td>
<td> </td>
<td>
<table>
<tr>
<td>.00</td><td>init</td><td></td></tr>
<tr>
<td>.62</td><td>LEFT</td><td>Fetched Schema</td></tr>
<tr>
<td>.75</td><td>TRANSFER</td><td>STORAGE_MIGRATION</td></tr>
<tr>
<td>.08</td><td>LEFT</td><td>Sql Run Complete for: Selecting DB</td></tr>
<tr>
<td>.00</td><td>LEFT</td><td>Sql Run Complete for: Remove table property</td></tr>
<tr>
<td>.00</td><td>LEFT</td><td>Sql Run Complete for: Rename table</td></tr>
<tr>
<td>.00</td><td>LEFT</td><td>Sql Run Complete for: Creating Table</td></tr>
<tr>
<td>.00</td><td>LEFT</td><td>Sql Run Complete for: Moving data to new Namespace</td></tr>
</table>
</td>
<td>
<table><tr>
<th colspan="2">RIGHT</th>
</tr>
<tr>
<td>hmsMirror_Metadata_Stage1</td>
<td>2023-04-26 12:07:02</td>
</tr>
</table></td>
<td>
<table></table></td>
<td>
<table><tr>
<th colspan="2">LEFT</th>
</tr>
<tr>
<td>Selecting DB</td>
<td>USE sm_orders</td>
</tr>
<tr>
<td>Remove table property</td>
<td>ALTER TABLE order_item_orc UNSET TBLPROPERTIES ("TRANSLATED_TO_EXTERNAL")</td>
</tr>
<tr>
<td>Rename table</td>
<td> ALTER TABLE order_item_orc RENAME TO order_item_orc_865f14c0bebf486eaa83ed418e886abf</td>
</tr>
<tr>
<td>Creating Table</td>
<td>CREATE EXTERNAL TABLE `order_item_orc`(
`order_id` string,
`order_item_id` string,
`product_id` string,
`quantity` bigint,
`cost` double)
ROW FORMAT SERDE
'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
'ofs://OHOME90/warehouse/tablespace/external/hive/sm_orders.db/order_item_orc'
TBLPROPERTIES (
'hmsMirror_Metadata_Stage1'='2023-04-26 12:07:02',
'TRANSLATED_TO_EXTERNAL'='TRUE',
'bucketing_version'='2',
'external.table.purge'='false'
)</td>
</tr>
<tr>
<td>Moving data to new Namespace</td>
<td>FROM order_item_orc_865f14c0bebf486eaa83ed418e886abf INSERT OVERWRITE TABLE order_item_orc SELECT *</td>
</tr>
<tr>
<td>Drop table</td>
<td>DROP TABLE IF EXISTS order_item_orc_865f14c0bebf486eaa83ed418e886abf</td>
</tr>
</table></td>
</tr>
<tr>
<td>order_item_small_orc</td>
<td>STORAGE_MIGRATION</td>
<td></td>
<td>
</td>
<td>SUCCESS</td>
<td>.02</td>
<td> </td>
<td>
<table>
<tr>
<td>.00</td><td>init</td><td></td></tr>
<tr>
<td>1.31</td><td>LEFT</td><td>Fetched Schema</td></tr>
<tr>
<td>.08</td><td>TRANSFER</td><td>STORAGE_MIGRATION</td></tr>
<tr>
<td>.02</td><td>LEFT</td><td>Sql Run Complete for: Selecting DB</td></tr>
<tr>
<td>.00</td><td>LEFT</td><td>Sql Run Complete for: Rename table</td></tr>
<tr>
<td>.00</td><td>LEFT</td><td>Sql Run Complete for: Creating Table</td></tr>
<tr>
<td>.00</td><td>LEFT</td><td>Sql Run Complete for: Moving data to new Namespace</td></tr>
</table>
</td>
<td>
<table><tr>
<th colspan="2">RIGHT</th>
</tr>
<tr>
<td>hmsMirror_Metadata_Stage1</td>
<td>2023-04-26 12:07:02</td>
</tr>
</table></td>
<td>
<table></table></td>
<td>
<table><tr>
<th colspan="2">LEFT</th>
</tr>
<tr>
<td>Selecting DB</td>
<td>USE sm_orders</td>
</tr>
<tr>
<td>Rename table</td>
<td> ALTER TABLE order_item_small_orc RENAME TO order_item_small_orc_fbd2ba431f0947f1b20165e2671a053b</td>
</tr>
<tr>
<td>Creating Table</td>
<td>CREATE EXTERNAL TABLE `order_item_small_orc`(
`order_id` string,
`order_item_id` string,
`product_id` string,
`quantity` bigint,
`cost` double)
ROW FORMAT SERDE
'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
'ofs://OHOME90/warehouse/tablespace/external/hive/sm_orders.db/order_item_small_orc'
TBLPROPERTIES (
'hmsMirror_Metadata_Stage1'='2023-04-26 12:07:02',
'bucketing_version'='2')</td>
</tr>
<tr>
<td>Moving data to new Namespace</td>
<td>FROM order_item_small_orc_fbd2ba431f0947f1b20165e2671a053b INSERT OVERWRITE TABLE order_item_small_orc SELECT *</td>
</tr>
<tr>
<td>Drop table</td>
<td>DROP TABLE IF EXISTS order_item_small_orc_fbd2ba431f0947f1b20165e2671a053b</td>
</tr>
</table></td>
</tr>
<tr>
<td>order_orc</td>
<td>STORAGE_MIGRATION</td>
<td></td>
<td>
</td>
<td>SUCCESS</td>
<td>.04</td>
<td> </td>
<td>
<table>
<tr>
<td>.00</td><td>init</td><td></td></tr>
<tr>
<td>.95</td><td>LEFT</td><td>Fetched Schema</td></tr>
<tr>
<td>.44</td><td>TRANSFER</td><td>STORAGE_MIGRATION</td></tr>
<tr>
<td>.04</td><td>LEFT</td><td>Sql Run Complete for: Selecting DB</td></tr>
<tr>
<td>.00</td><td>LEFT</td><td>Sql Run Complete for: Rename table</td></tr>
<tr>
<td>.00</td><td>LEFT</td><td>Sql Run Complete for: Creating Table</td></tr>
<tr>
<td>.00</td><td>LEFT</td><td>Sql Run Complete for: Moving data to new Namespace</td></tr>
</table>
</td>
<td>
<table><tr>
<th colspan="2">RIGHT</th>
</tr>
<tr>
<td>hmsMirror_Metadata_Stage1</td>
<td>2023-04-26 12:07:02</td>
</tr>
</table></td>
<td>
<table></table></td>
<td>
<table><tr>
<th colspan="2">LEFT</th>
</tr>
<tr>
<td>Selecting DB</td>
<td>USE sm_orders</td>
</tr>
<tr>
<td>Rename table</td>
<td> ALTER TABLE order_orc RENAME TO order_orc_bac0df8d058742b9ae1c6295d080be80</td>
</tr>
<tr>
<td>Creating Table</td>
<td>CREATE EXTERNAL TABLE `order_orc`(
`id` string,
`user_id` string,
`order_date` date,
`status` string)
ROW FORMAT SERDE
'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
'ofs://OHOME90/warehouse/tablespace/external/hive/sm_orders.db/order_orc'
TBLPROPERTIES (
'hmsMirror_Metadata_Stage1'='2023-04-26 12:07:02',
'bucketing_version'='2')</td>
</tr>
<tr>
<td>Moving data to new Namespace</td>
<td>FROM order_orc_bac0df8d058742b9ae1c6295d080be80 INSERT OVERWRITE TABLE order_orc SELECT *</td>
</tr>
<tr>
<td>Drop table</td>
<td>DROP TABLE IF EXISTS order_orc_bac0df8d058742b9ae1c6295d080be80</td>
</tr>
</table></td>
</tr>
<tr>
<td>order_small_orc</td>
<td>STORAGE_MIGRATION</td>
<td></td>
<td>
</td>
<td>SUCCESS</td>
<td>.02</td>
<td> </td>
<td>
<table>
<tr>
<td>.00</td><td>init</td><td></td></tr>
<tr>
<td>1.30</td><td>LEFT</td><td>Fetched Schema</td></tr>
<tr>
<td>.11</td><td>TRANSFER</td><td>STORAGE_MIGRATION</td></tr>
<tr>
<td>.02</td><td>LEFT</td><td>Sql Run Complete for: Selecting DB</td></tr>
<tr>
<td>.00</td><td>LEFT</td><td>Sql Run Complete for: Rename table</td></tr>
<tr>
<td>.00</td><td>LEFT</td><td>Sql Run Complete for: Creating Table</td></tr>
<tr>
<td>.00</td><td>LEFT</td><td>Sql Run Complete for: Moving data to new Namespace</td></tr>
</table>
</td>
<td>
<table><tr>
<th colspan="2">RIGHT</th>
</tr>
<tr>
<td>hmsMirror_Metadata_Stage1</td>
<td>2023-04-26 12:07:02</td>
</tr>
</table></td>
<td>
<table></table></td>
<td>
<table><tr>
<th colspan="2">LEFT</th>
</tr>
<tr>
<td>Selecting DB</td>
<td>USE sm_orders</td>
</tr>
<tr>
<td>Rename table</td>
<td> ALTER TABLE order_small_orc RENAME TO order_small_orc_43a1edde205d4e338fa6502b2b0f5fe8</td>
</tr>
<tr>
<td>Creating Table</td>
<td>CREATE EXTERNAL TABLE `order_small_orc`(
`id` string,
`user_id` string,
`order_date` date,
`status` string)
ROW FORMAT SERDE
'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
'ofs://OHOME90/warehouse/tablespace/external/hive/sm_orders.db/order_small_orc'
TBLPROPERTIES (
'hmsMirror_Metadata_Stage1'='2023-04-26 12:07:02',
'bucketing_version'='2')</td>
</tr>
<tr>
<td>Moving data to new Namespace</td>
<td>FROM order_small_orc_43a1edde205d4e338fa6502b2b0f5fe8 INSERT OVERWRITE TABLE order_small_orc SELECT *</td>
</tr>
<tr>
<td>Drop table</td>
<td>DROP TABLE IF EXISTS order_small_orc_43a1edde205d4e338fa6502b2b0f5fe8</td>
</tr>
</table></td>
</tr>
<tr>
<td>order_src</td>
<td>STORAGE_MIGRATION</td>
<td></td>
<td>
</td>
<td>SUCCESS</td>
<td>.03</td>
<td> </td>
<td>
<table>
<tr>
<td>.00</td><td>init</td><td></td></tr>
<tr>
<td>1.29</td><td>LEFT</td><td>Fetched Schema</td></tr>
<tr>
<td>.12</td><td>TRANSFER</td><td>STORAGE_MIGRATION</td></tr>
<tr>
<td>.03</td><td>LEFT</td><td>Sql Run Complete for: Selecting DB</td></tr>
<tr>
<td>.00</td><td>LEFT</td><td>Sql Run Complete for: Rename table</td></tr>
<tr>
<td>.00</td><td>LEFT</td><td>Sql Run Complete for: Creating Table</td></tr>
<tr>
<td>.00</td><td>LEFT</td><td>Sql Run Complete for: Moving data to new Namespace</td></tr>
</table>
</td>
<td>
<table><tr>
<th colspan="2">RIGHT</th>
</tr>
<tr>
<td>hmsMirror_Metadata_Stage1</td>
<td>2023-04-26 12:07:02</td>
</tr>
</table></td>
<td>
<table><tr>
<th>RIGHT</th>
</tr>
<tr>
<td>Feature (STRUCT_ESCAPE) was found applicable and adjustments applied. Reserved/Key words in a tables field definition need to be escaped.  If they are NOT, this process will escape them and post a warning about the change.</td>
</tr>
</table></td>
<td>
<table><tr>
<th colspan="2">LEFT</th>
</tr>
<tr>
<td>Selecting DB</td>
<td>USE sm_orders</td>
</tr>
<tr>
<td>Rename table</td>
<td> ALTER TABLE order_src RENAME TO order_src_b14312a499594ec1811ef13b3a687d2c</td>
</tr>
<tr>
<td>Creating Table</td>
<td>CREATE EXTERNAL TABLE `order_src`(
`id` string COMMENT 'from deserializer',
`user_id` string COMMENT 'from deserializer',
`order_date` date COMMENT 'from deserializer',
`status` string COMMENT 'from deserializer',
`order_items` array<struct<`order_item_id`:string, `product_id`:string, `quantity`:bigint, `cost`:double>> COMMENT 'from deserializer')
ROW FORMAT SERDE
'org.apache.hadoop.hive.serde2.JsonSerDe'
STORED AS INPUTFORMAT
'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
'ofs://OHOME90/user/dstreev/datasets/orders_small'
TBLPROPERTIES (
'hmsMirror_Metadata_Stage1'='2023-04-26 12:07:02',
'bucketing_version'='2')</td>
</tr>
<tr>
<td>Moving data to new Namespace</td>
<td>FROM order_src_b14312a499594ec1811ef13b3a687d2c INSERT OVERWRITE TABLE order_src SELECT *</td>
</tr>
<tr>
<td>Drop table</td>
<td>DROP TABLE IF EXISTS order_src_b14312a499594ec1811ef13b3a687d2c</td>
</tr>
</table></td>
</tr>
</table>
