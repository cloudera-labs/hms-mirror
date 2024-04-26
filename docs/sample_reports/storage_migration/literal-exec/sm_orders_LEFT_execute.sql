-- EXECUTION script for sm_orders on LEFT cluster

-- 2023-04-26_12-08-09-- These are the command run on the LEFT cluster when `-e` is used.
-- Alter Database Location
ALTER DATABASE sm_orders SET LOCATION "ofs://OHOME90/warehouse/tablespace/external/hive/sm_orders.db";
-- Alter Database Managed Location
ALTER DATABASE sm_orders SET MANAGEDLOCATION "ofs://OHOME90/warehouse/tablespace/managed/hive/sm_orders.db";

--    Table: mngd_order_item_orc
USE sm_orders;
 ALTER TABLE mngd_order_item_orc RENAME TO mngd_order_item_orc_f0ea64e31cf147edb4afbd4bbe2eafa5;
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
'hmsMirror_Metadata_Stage1'='2023-04-26 12:07:32',
'bucketing_version'='2',
'transactional'='true',
'transactional_properties'='insert_only'
);
FROM mngd_order_item_orc_f0ea64e31cf147edb4afbd4bbe2eafa5 INSERT OVERWRITE TABLE mngd_order_item_orc SELECT *;

--    Table: mngd_order_item_small_orc
USE sm_orders;
 ALTER TABLE mngd_order_item_small_orc RENAME TO mngd_order_item_small_orc_6a146a8ae0384cc09786ff248e659bf7;
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
'hmsMirror_Metadata_Stage1'='2023-04-26 12:07:32',
'bucketing_version'='2',
'transactional'='true',
'transactional_properties'='insert_only'
);
FROM mngd_order_item_small_orc_6a146a8ae0384cc09786ff248e659bf7 INSERT OVERWRITE TABLE mngd_order_item_small_orc SELECT *;

--    Table: mngd_order_orc
USE sm_orders;
 ALTER TABLE mngd_order_orc RENAME TO mngd_order_orc_66603a32bb344a4c86b623fa0b808703;
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
'hmsMirror_Metadata_Stage1'='2023-04-26 12:07:32',
'bucketing_version'='2',
'transactional'='true',
'transactional_properties'='insert_only'
);
FROM mngd_order_orc_66603a32bb344a4c86b623fa0b808703 INSERT OVERWRITE TABLE mngd_order_orc SELECT *;

--    Table: mngd_order_small_orc
USE sm_orders;
 ALTER TABLE mngd_order_small_orc RENAME TO mngd_order_small_orc_3bf1ff616b2e4e5fadddaec689a22d75;
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
'hmsMirror_Metadata_Stage1'='2023-04-26 12:07:32',
'bucketing_version'='2',
'transactional'='true',
'transactional_properties'='insert_only'
);
FROM mngd_order_small_orc_3bf1ff616b2e4e5fadddaec689a22d75 INSERT OVERWRITE TABLE mngd_order_small_orc SELECT *;

--    Table: order_item_orc
USE sm_orders;
ALTER TABLE order_item_orc UNSET TBLPROPERTIES ("TRANSLATED_TO_EXTERNAL");
 ALTER TABLE order_item_orc RENAME TO order_item_orc_1e592260cbae4600b8f6c24bb168b057;
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
'hmsMirror_Metadata_Stage1'='2023-04-26 12:07:52',
'TRANSLATED_TO_EXTERNAL'='TRUE',
'bucketing_version'='2',
'external.table.purge'='false'
);
FROM order_item_orc_1e592260cbae4600b8f6c24bb168b057 INSERT OVERWRITE TABLE order_item_orc SELECT *;

--    Table: order_item_small_orc
USE sm_orders;
 ALTER TABLE order_item_small_orc RENAME TO order_item_small_orc_f8412906c3994f88b1673264fd7b42f1;
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
'hmsMirror_Metadata_Stage1'='2023-04-26 12:07:54',
'bucketing_version'='2');
FROM order_item_small_orc_f8412906c3994f88b1673264fd7b42f1 INSERT OVERWRITE TABLE order_item_small_orc SELECT *;

--    Table: order_orc
USE sm_orders;
 ALTER TABLE order_orc RENAME TO order_orc_525c9296e82142ada064dfeb9306a9f9;
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
'hmsMirror_Metadata_Stage1'='2023-04-26 12:07:54',
'bucketing_version'='2');
FROM order_orc_525c9296e82142ada064dfeb9306a9f9 INSERT OVERWRITE TABLE order_orc SELECT *;

--    Table: order_small_orc
USE sm_orders;
 ALTER TABLE order_small_orc RENAME TO order_small_orc_035800882a7c44c8996d7e552a6cb327;
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
'hmsMirror_Metadata_Stage1'='2023-04-26 12:07:54',
'bucketing_version'='2');
FROM order_small_orc_035800882a7c44c8996d7e552a6cb327 INSERT OVERWRITE TABLE order_small_orc SELECT *;

--    Table: order_src
USE sm_orders;
 ALTER TABLE order_src RENAME TO order_src_1fb2108275ce44b0a12bf0a38f7cc092;
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
'hmsMirror_Metadata_Stage1'='2023-04-26 12:07:58',
'bucketing_version'='2');
FROM order_src_1fb2108275ce44b0a12bf0a38f7cc092 INSERT OVERWRITE TABLE order_src SELECT *;
