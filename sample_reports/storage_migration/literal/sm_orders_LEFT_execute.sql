-- EXECUTION script for sm_orders on LEFT cluster

-- 2023-04-26_12-07-03-- These are the command run on the LEFT cluster when `-e` is used.
-- Alter Database Location
ALTER DATABASE sm_orders SET LOCATION "ofs://OHOME90/warehouse/tablespace/external/hive/sm_orders.db";
-- Alter Database Managed Location
ALTER DATABASE sm_orders SET MANAGEDLOCATION "ofs://OHOME90/warehouse/tablespace/managed/hive/sm_orders.db";

--    Table: mngd_order_item_orc
USE sm_orders;
 ALTER TABLE mngd_order_item_orc RENAME TO mngd_order_item_orc_b723de0a4008437095141f39c24349ec;
CREATE TABLE `mngd_order_item_orc`(
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
);
FROM mngd_order_item_orc_b723de0a4008437095141f39c24349ec INSERT OVERWRITE TABLE mngd_order_item_orc SELECT *;

--    Table: mngd_order_item_small_orc
USE sm_orders;
 ALTER TABLE mngd_order_item_small_orc RENAME TO mngd_order_item_small_orc_8108bc20c7ef4fc59611d25459fecf63;
CREATE TABLE `mngd_order_item_small_orc`(
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
);
FROM mngd_order_item_small_orc_8108bc20c7ef4fc59611d25459fecf63 INSERT OVERWRITE TABLE mngd_order_item_small_orc SELECT *;

--    Table: mngd_order_orc
USE sm_orders;
 ALTER TABLE mngd_order_orc RENAME TO mngd_order_orc_8e63f5ebb9394e12a77f4e3f62eb3b52;
CREATE TABLE `mngd_order_orc`(
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
);
FROM mngd_order_orc_8e63f5ebb9394e12a77f4e3f62eb3b52 INSERT OVERWRITE TABLE mngd_order_orc SELECT *;

--    Table: mngd_order_small_orc
USE sm_orders;
 ALTER TABLE mngd_order_small_orc RENAME TO mngd_order_small_orc_be1610dd2d4a43128d9b1adf12cddce8;
CREATE TABLE `mngd_order_small_orc`(
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
);
FROM mngd_order_small_orc_be1610dd2d4a43128d9b1adf12cddce8 INSERT OVERWRITE TABLE mngd_order_small_orc SELECT *;

--    Table: order_item_orc
USE sm_orders;
ALTER TABLE order_item_orc UNSET TBLPROPERTIES ("TRANSLATED_TO_EXTERNAL");
 ALTER TABLE order_item_orc RENAME TO order_item_orc_865f14c0bebf486eaa83ed418e886abf;
CREATE EXTERNAL TABLE `order_item_orc`(
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
);
FROM order_item_orc_865f14c0bebf486eaa83ed418e886abf INSERT OVERWRITE TABLE order_item_orc SELECT *;

--    Table: order_item_small_orc
USE sm_orders;
 ALTER TABLE order_item_small_orc RENAME TO order_item_small_orc_fbd2ba431f0947f1b20165e2671a053b;
CREATE EXTERNAL TABLE `order_item_small_orc`(
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
'bucketing_version'='2');
FROM order_item_small_orc_fbd2ba431f0947f1b20165e2671a053b INSERT OVERWRITE TABLE order_item_small_orc SELECT *;

--    Table: order_orc
USE sm_orders;
 ALTER TABLE order_orc RENAME TO order_orc_bac0df8d058742b9ae1c6295d080be80;
CREATE EXTERNAL TABLE `order_orc`(
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
'bucketing_version'='2');
FROM order_orc_bac0df8d058742b9ae1c6295d080be80 INSERT OVERWRITE TABLE order_orc SELECT *;

--    Table: order_small_orc
USE sm_orders;
 ALTER TABLE order_small_orc RENAME TO order_small_orc_43a1edde205d4e338fa6502b2b0f5fe8;
CREATE EXTERNAL TABLE `order_small_orc`(
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
'bucketing_version'='2');
FROM order_small_orc_43a1edde205d4e338fa6502b2b0f5fe8 INSERT OVERWRITE TABLE order_small_orc SELECT *;

--    Table: order_src
USE sm_orders;
 ALTER TABLE order_src RENAME TO order_src_b14312a499594ec1811ef13b3a687d2c;
CREATE EXTERNAL TABLE `order_src`(
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
'bucketing_version'='2');
FROM order_src_b14312a499594ec1811ef13b3a687d2c INSERT OVERWRITE TABLE order_src SELECT *;
