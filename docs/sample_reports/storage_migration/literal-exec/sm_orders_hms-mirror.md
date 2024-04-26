# HMS-Mirror for: sm_orders

v.1.5.4.6-SNAPSHOT
---
## Run Log

| Date | Elapsed Time |
|:---|:---|
| 2023-04-26 12:08:09 | 39.21 secs |

## Config:
```
---
acceptance:
  silentOverride: true
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
  \ -o, conversion/literal-exec, -e, --accept]"
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
execute: true
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
<td>20.42</td>
<td> </td>
<td>
<table>
<tr>
<td>.00</td><td>init</td><td></td></tr>
<tr>
<td>.39</td><td>LEFT</td><td>Fetched Schema</td></tr>
<tr>
<td>.00</td><td>TRANSACTIONAL</td><td>true</td></tr>
<tr>
<td>1.01</td><td>TRANSFER</td><td>STORAGE_MIGRATION</td></tr>
<tr>
<td>.06</td><td>LEFT</td><td>Sql Run Complete for: Selecting DB</td></tr>
<tr>
<td>.28</td><td>LEFT</td><td>Sql Run Complete for: Rename table</td></tr>
<tr>
<td>.18</td><td>LEFT</td><td>Sql Run Complete for: Creating Table</td></tr>
<tr>
<td>19.86</td><td>LEFT</td><td>Sql Run Complete for: Moving data to new Namespace</td></tr>
</table>
</td>
<td>
<table><tr>
<th colspan="2">RIGHT</th>
</tr>
<tr>
<td>hmsMirror_Metadata_Stage1</td>
<td>2023-04-26 12:07:32</td>
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
<td> ALTER TABLE mngd_order_item_orc RENAME TO mngd_order_item_orc_f0ea64e31cf147edb4afbd4bbe2eafa5</td>
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
'hmsMirror_Metadata_Stage1'='2023-04-26 12:07:32',
'bucketing_version'='2',
'transactional'='true',
'transactional_properties'='insert_only'
)</td>
</tr>
<tr>
<td>Moving data to new Namespace</td>
<td>FROM mngd_order_item_orc_f0ea64e31cf147edb4afbd4bbe2eafa5 INSERT OVERWRITE TABLE mngd_order_item_orc SELECT *</td>
</tr>
<tr>
<td>Drop table</td>
<td>DROP TABLE IF EXISTS mngd_order_item_orc_f0ea64e31cf147edb4afbd4bbe2eafa5</td>
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
<td>22.30</td>
<td> </td>
<td>
<table>
<tr>
<td>.00</td><td>init</td><td></td></tr>
<tr>
<td>.83</td><td>LEFT</td><td>Fetched Schema</td></tr>
<tr>
<td>.00</td><td>TRANSACTIONAL</td><td>true</td></tr>
<tr>
<td>.57</td><td>TRANSFER</td><td>STORAGE_MIGRATION</td></tr>
<tr>
<td>.06</td><td>LEFT</td><td>Sql Run Complete for: Selecting DB</td></tr>
<tr>
<td>.29</td><td>LEFT</td><td>Sql Run Complete for: Rename table</td></tr>
<tr>
<td>.18</td><td>LEFT</td><td>Sql Run Complete for: Creating Table</td></tr>
<tr>
<td>21.75</td><td>LEFT</td><td>Sql Run Complete for: Moving data to new Namespace</td></tr>
</table>
</td>
<td>
<table><tr>
<th colspan="2">RIGHT</th>
</tr>
<tr>
<td>hmsMirror_Metadata_Stage1</td>
<td>2023-04-26 12:07:32</td>
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
<td> ALTER TABLE mngd_order_item_small_orc RENAME TO mngd_order_item_small_orc_6a146a8ae0384cc09786ff248e659bf7</td>
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
'hmsMirror_Metadata_Stage1'='2023-04-26 12:07:32',
'bucketing_version'='2',
'transactional'='true',
'transactional_properties'='insert_only'
)</td>
</tr>
<tr>
<td>Moving data to new Namespace</td>
<td>FROM mngd_order_item_small_orc_6a146a8ae0384cc09786ff248e659bf7 INSERT OVERWRITE TABLE mngd_order_item_small_orc SELECT *</td>
</tr>
<tr>
<td>Drop table</td>
<td>DROP TABLE IF EXISTS mngd_order_item_small_orc_6a146a8ae0384cc09786ff248e659bf7</td>
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
<td>22.51</td>
<td> </td>
<td>
<table>
<tr>
<td>.00</td><td>init</td><td></td></tr>
<tr>
<td>1.10</td><td>LEFT</td><td>Fetched Schema</td></tr>
<tr>
<td>.00</td><td>TRANSACTIONAL</td><td>true</td></tr>
<tr>
<td>.30</td><td>TRANSFER</td><td>STORAGE_MIGRATION</td></tr>
<tr>
<td>.06</td><td>LEFT</td><td>Sql Run Complete for: Selecting DB</td></tr>
<tr>
<td>.29</td><td>LEFT</td><td>Sql Run Complete for: Rename table</td></tr>
<tr>
<td>.17</td><td>LEFT</td><td>Sql Run Complete for: Creating Table</td></tr>
<tr>
<td>21.96</td><td>LEFT</td><td>Sql Run Complete for: Moving data to new Namespace</td></tr>
</table>
</td>
<td>
<table><tr>
<th colspan="2">RIGHT</th>
</tr>
<tr>
<td>hmsMirror_Metadata_Stage1</td>
<td>2023-04-26 12:07:32</td>
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
<td> ALTER TABLE mngd_order_orc RENAME TO mngd_order_orc_66603a32bb344a4c86b623fa0b808703</td>
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
'hmsMirror_Metadata_Stage1'='2023-04-26 12:07:32',
'bucketing_version'='2',
'transactional'='true',
'transactional_properties'='insert_only'
)</td>
</tr>
<tr>
<td>Moving data to new Namespace</td>
<td>FROM mngd_order_orc_66603a32bb344a4c86b623fa0b808703 INSERT OVERWRITE TABLE mngd_order_orc SELECT *</td>
</tr>
<tr>
<td>Drop table</td>
<td>DROP TABLE IF EXISTS mngd_order_orc_66603a32bb344a4c86b623fa0b808703</td>
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
<td>22.43</td>
<td> </td>
<td>
<table>
<tr>
<td>.00</td><td>init</td><td></td></tr>
<tr>
<td>1.10</td><td>LEFT</td><td>Fetched Schema</td></tr>
<tr>
<td>.00</td><td>TRANSACTIONAL</td><td>true</td></tr>
<tr>
<td>.31</td><td>TRANSFER</td><td>STORAGE_MIGRATION</td></tr>
<tr>
<td>.06</td><td>LEFT</td><td>Sql Run Complete for: Selecting DB</td></tr>
<tr>
<td>.28</td><td>LEFT</td><td>Sql Run Complete for: Rename table</td></tr>
<tr>
<td>.18</td><td>LEFT</td><td>Sql Run Complete for: Creating Table</td></tr>
<tr>
<td>21.88</td><td>LEFT</td><td>Sql Run Complete for: Moving data to new Namespace</td></tr>
</table>
</td>
<td>
<table><tr>
<th colspan="2">RIGHT</th>
</tr>
<tr>
<td>hmsMirror_Metadata_Stage1</td>
<td>2023-04-26 12:07:32</td>
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
<td> ALTER TABLE mngd_order_small_orc RENAME TO mngd_order_small_orc_3bf1ff616b2e4e5fadddaec689a22d75</td>
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
'hmsMirror_Metadata_Stage1'='2023-04-26 12:07:32',
'bucketing_version'='2',
'transactional'='true',
'transactional_properties'='insert_only'
)</td>
</tr>
<tr>
<td>Moving data to new Namespace</td>
<td>FROM mngd_order_small_orc_3bf1ff616b2e4e5fadddaec689a22d75 INSERT OVERWRITE TABLE mngd_order_small_orc SELECT *</td>
</tr>
<tr>
<td>Drop table</td>
<td>DROP TABLE IF EXISTS mngd_order_small_orc_3bf1ff616b2e4e5fadddaec689a22d75</td>
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
<td>12.02</td>
<td> </td>
<td>
<table>
<tr>
<td>.00</td><td>init</td><td></td></tr>
<tr>
<td>.75</td><td>LEFT</td><td>Fetched Schema</td></tr>
<tr>
<td>21.08</td><td>TRANSFER</td><td>STORAGE_MIGRATION</td></tr>
<tr>
<td>.05</td><td>LEFT</td><td>Sql Run Complete for: Selecting DB</td></tr>
<tr>
<td>.18</td><td>LEFT</td><td>Sql Run Complete for: Remove table property</td></tr>
<tr>
<td>.20</td><td>LEFT</td><td>Sql Run Complete for: Rename table</td></tr>
<tr>
<td>.18</td><td>LEFT</td><td>Sql Run Complete for: Creating Table</td></tr>
<tr>
<td>11.38</td><td>LEFT</td><td>Sql Run Complete for: Moving data to new Namespace</td></tr>
</table>
</td>
<td>
<table><tr>
<th colspan="2">RIGHT</th>
</tr>
<tr>
<td>hmsMirror_Metadata_Stage1</td>
<td>2023-04-26 12:07:52</td>
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
<td> ALTER TABLE order_item_orc RENAME TO order_item_orc_1e592260cbae4600b8f6c24bb168b057</td>
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
'hmsMirror_Metadata_Stage1'='2023-04-26 12:07:52',
'TRANSLATED_TO_EXTERNAL'='TRUE',
'bucketing_version'='2',
'external.table.purge'='false'
)</td>
</tr>
<tr>
<td>Moving data to new Namespace</td>
<td>FROM order_item_orc_1e592260cbae4600b8f6c24bb168b057 INSERT OVERWRITE TABLE order_item_orc SELECT *</td>
</tr>
<tr>
<td>Drop table</td>
<td>DROP TABLE IF EXISTS order_item_orc_1e592260cbae4600b8f6c24bb168b057</td>
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
<td>9.54</td>
<td> </td>
<td>
<table>
<tr>
<td>.00</td><td>init</td><td></td></tr>
<tr>
<td>1.39</td><td>LEFT</td><td>Fetched Schema</td></tr>
<tr>
<td>22.32</td><td>TRANSFER</td><td>STORAGE_MIGRATION</td></tr>
<tr>
<td>.03</td><td>LEFT</td><td>Sql Run Complete for: Selecting DB</td></tr>
<tr>
<td>.19</td><td>LEFT</td><td>Sql Run Complete for: Rename table</td></tr>
<tr>
<td>.17</td><td>LEFT</td><td>Sql Run Complete for: Creating Table</td></tr>
<tr>
<td>9.14</td><td>LEFT</td><td>Sql Run Complete for: Moving data to new Namespace</td></tr>
</table>
</td>
<td>
<table><tr>
<th colspan="2">RIGHT</th>
</tr>
<tr>
<td>hmsMirror_Metadata_Stage1</td>
<td>2023-04-26 12:07:54</td>
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
<td> ALTER TABLE order_item_small_orc RENAME TO order_item_small_orc_f8412906c3994f88b1673264fd7b42f1</td>
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
'hmsMirror_Metadata_Stage1'='2023-04-26 12:07:54',
'bucketing_version'='2')</td>
</tr>
<tr>
<td>Moving data to new Namespace</td>
<td>FROM order_item_small_orc_f8412906c3994f88b1673264fd7b42f1 INSERT OVERWRITE TABLE order_item_small_orc SELECT *</td>
</tr>
<tr>
<td>Drop table</td>
<td>DROP TABLE IF EXISTS order_item_small_orc_f8412906c3994f88b1673264fd7b42f1</td>
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
<td>3.66</td>
<td> </td>
<td>
<table>
<tr>
<td>.00</td><td>init</td><td></td></tr>
<tr>
<td>1.08</td><td>LEFT</td><td>Fetched Schema</td></tr>
<tr>
<td>22.75</td><td>TRANSFER</td><td>STORAGE_MIGRATION</td></tr>
<tr>
<td>.04</td><td>LEFT</td><td>Sql Run Complete for: Selecting DB</td></tr>
<tr>
<td>.19</td><td>LEFT</td><td>Sql Run Complete for: Rename table</td></tr>
<tr>
<td>.22</td><td>LEFT</td><td>Sql Run Complete for: Creating Table</td></tr>
<tr>
<td>3.17</td><td>LEFT</td><td>Sql Run Complete for: Moving data to new Namespace</td></tr>
</table>
</td>
<td>
<table><tr>
<th colspan="2">RIGHT</th>
</tr>
<tr>
<td>hmsMirror_Metadata_Stage1</td>
<td>2023-04-26 12:07:54</td>
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
<td> ALTER TABLE order_orc RENAME TO order_orc_525c9296e82142ada064dfeb9306a9f9</td>
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
'hmsMirror_Metadata_Stage1'='2023-04-26 12:07:54',
'bucketing_version'='2')</td>
</tr>
<tr>
<td>Moving data to new Namespace</td>
<td>FROM order_orc_525c9296e82142ada064dfeb9306a9f9 INSERT OVERWRITE TABLE order_orc SELECT *</td>
</tr>
<tr>
<td>Drop table</td>
<td>DROP TABLE IF EXISTS order_orc_525c9296e82142ada064dfeb9306a9f9</td>
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
<td>4.97</td>
<td> </td>
<td>
<table>
<tr>
<td>.00</td><td>init</td><td></td></tr>
<tr>
<td>1.38</td><td>LEFT</td><td>Fetched Schema</td></tr>
<tr>
<td>22.54</td><td>TRANSFER</td><td>STORAGE_MIGRATION</td></tr>
<tr>
<td>.04</td><td>LEFT</td><td>Sql Run Complete for: Selecting DB</td></tr>
<tr>
<td>.19</td><td>LEFT</td><td>Sql Run Complete for: Rename table</td></tr>
<tr>
<td>.16</td><td>LEFT</td><td>Sql Run Complete for: Creating Table</td></tr>
<tr>
<td>4.55</td><td>LEFT</td><td>Sql Run Complete for: Moving data to new Namespace</td></tr>
</table>
</td>
<td>
<table><tr>
<th colspan="2">RIGHT</th>
</tr>
<tr>
<td>hmsMirror_Metadata_Stage1</td>
<td>2023-04-26 12:07:54</td>
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
<td> ALTER TABLE order_small_orc RENAME TO order_small_orc_035800882a7c44c8996d7e552a6cb327</td>
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
'hmsMirror_Metadata_Stage1'='2023-04-26 12:07:54',
'bucketing_version'='2')</td>
</tr>
<tr>
<td>Moving data to new Namespace</td>
<td>FROM order_small_orc_035800882a7c44c8996d7e552a6cb327 INSERT OVERWRITE TABLE order_small_orc SELECT *</td>
</tr>
<tr>
<td>Drop table</td>
<td>DROP TABLE IF EXISTS order_small_orc_035800882a7c44c8996d7e552a6cb327</td>
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
<td>11.39</td>
<td> </td>
<td>
<table>
<tr>
<td>.00</td><td>init</td><td></td></tr>
<tr>
<td>1.38</td><td>LEFT</td><td>Fetched Schema</td></tr>
<tr>
<td>26.10</td><td>TRANSFER</td><td>STORAGE_MIGRATION</td></tr>
<tr>
<td>.04</td><td>LEFT</td><td>Sql Run Complete for: Selecting DB</td></tr>
<tr>
<td>.17</td><td>LEFT</td><td>Sql Run Complete for: Rename table</td></tr>
<tr>
<td>.19</td><td>LEFT</td><td>Sql Run Complete for: Creating Table</td></tr>
<tr>
<td>10.96</td><td>LEFT</td><td>Sql Run Complete for: Moving data to new Namespace</td></tr>
</table>
</td>
<td>
<table><tr>
<th colspan="2">RIGHT</th>
</tr>
<tr>
<td>hmsMirror_Metadata_Stage1</td>
<td>2023-04-26 12:07:58</td>
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
<td> ALTER TABLE order_src RENAME TO order_src_1fb2108275ce44b0a12bf0a38f7cc092</td>
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
'hmsMirror_Metadata_Stage1'='2023-04-26 12:07:58',
'bucketing_version'='2')</td>
</tr>
<tr>
<td>Moving data to new Namespace</td>
<td>FROM order_src_1fb2108275ce44b0a12bf0a38f7cc092 INSERT OVERWRITE TABLE order_src SELECT *</td>
</tr>
<tr>
<td>Drop table</td>
<td>DROP TABLE IF EXISTS order_src_1fb2108275ce44b0a12bf0a38f7cc092</td>
</tr>
</table></td>
</tr>
</table>
