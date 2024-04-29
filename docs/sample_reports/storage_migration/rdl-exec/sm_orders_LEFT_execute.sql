-- EXECUTION script for sm_orders on LEFT cluster

-- 2023-04-26_12-10-21-- These are the command run on the LEFT cluster when `-e` is used.
-- Alter Database Location
ALTER DATABASE sm_orders SET LOCATION "ofs://OHOME90/finance/operations-ext/sm_orders.db";
-- Alter Database Managed Location
ALTER DATABASE sm_orders SET MANAGEDLOCATION "ofs://OHOME90/finance/operations-mngd/sm_orders.db";

--    Table: mngd_order_item_orc
USE sm_orders;
 ALTER TABLE mngd_order_item_orc RENAME TO mngd_order_item_orc_bba5f0beaa394d59aa64f2d715939461;
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
'hmsMirror_Metadata_Stage1'='2023-04-26 12:09:46',
'bucketing_version'='2',
'transactional'='true',
'transactional_properties'='insert_only'
);
FROM mngd_order_item_orc_bba5f0beaa394d59aa64f2d715939461 INSERT OVERWRITE TABLE mngd_order_item_orc SELECT *;

--    Table: mngd_order_item_small_orc
USE sm_orders;
 ALTER TABLE mngd_order_item_small_orc RENAME TO mngd_order_item_small_orc_e61247eaaea945e1b332ad9ba42cc532;
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
'hmsMirror_Metadata_Stage1'='2023-04-26 12:09:46',
'bucketing_version'='2',
'transactional'='true',
'transactional_properties'='insert_only'
);
FROM mngd_order_item_small_orc_e61247eaaea945e1b332ad9ba42cc532 INSERT OVERWRITE TABLE mngd_order_item_small_orc SELECT *;

--    Table: mngd_order_orc
USE sm_orders;
 ALTER TABLE mngd_order_orc RENAME TO mngd_order_orc_261cc27a86dd4ab2b1a7ac38393c431e;
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
'hmsMirror_Metadata_Stage1'='2023-04-26 12:09:46',
'bucketing_version'='2',
'transactional'='true',
'transactional_properties'='insert_only'
);
FROM mngd_order_orc_261cc27a86dd4ab2b1a7ac38393c431e INSERT OVERWRITE TABLE mngd_order_orc SELECT *;

--    Table: mngd_order_small_orc
USE sm_orders;
 ALTER TABLE mngd_order_small_orc RENAME TO mngd_order_small_orc_87fcf4d70fe644408b82ee5376d226f1;
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
'hmsMirror_Metadata_Stage1'='2023-04-26 12:09:46',
'bucketing_version'='2',
'transactional'='true',
'transactional_properties'='insert_only'
);
FROM mngd_order_small_orc_87fcf4d70fe644408b82ee5376d226f1 INSERT OVERWRITE TABLE mngd_order_small_orc SELECT *;

--    Table: order_item_orc
USE sm_orders;
ALTER TABLE order_item_orc UNSET TBLPROPERTIES ("TRANSLATED_TO_EXTERNAL");
 ALTER TABLE order_item_orc RENAME TO order_item_orc_d423fe9a47a14c8a9bbab655c3649c63;
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
TBLPROPERTIES (
'hmsMirror_Metadata_Stage1'='2023-04-26 12:10:05',
'TRANSLATED_TO_EXTERNAL'='TRUE',
'bucketing_version'='2',
'external.table.purge'='false'
);
FROM order_item_orc_d423fe9a47a14c8a9bbab655c3649c63 INSERT OVERWRITE TABLE order_item_orc SELECT *;

--    Table: order_item_small_orc
USE sm_orders;
 ALTER TABLE order_item_small_orc RENAME TO order_item_small_orc_a5381521ed5d45febaddc9f0e43c8963;
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
TBLPROPERTIES (
'hmsMirror_Metadata_Stage1'='2023-04-26 12:10:05',
'bucketing_version'='2');
FROM order_item_small_orc_a5381521ed5d45febaddc9f0e43c8963 INSERT OVERWRITE TABLE order_item_small_orc SELECT *;

--    Table: order_orc
USE sm_orders;
 ALTER TABLE order_orc RENAME TO order_orc_20b2758e3f4441d487c4061372480263;
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
TBLPROPERTIES (
'hmsMirror_Metadata_Stage1'='2023-04-26 12:10:06',
'bucketing_version'='2');
FROM order_orc_20b2758e3f4441d487c4061372480263 INSERT OVERWRITE TABLE order_orc SELECT *;

--    Table: order_small_orc
USE sm_orders;
 ALTER TABLE order_small_orc RENAME TO order_small_orc_eb7078c838524dd79d0e6ced6500a309;
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
TBLPROPERTIES (
'hmsMirror_Metadata_Stage1'='2023-04-26 12:10:08',
'bucketing_version'='2');
FROM order_small_orc_eb7078c838524dd79d0e6ced6500a309 INSERT OVERWRITE TABLE order_small_orc SELECT *;

--    Table: order_src
USE sm_orders;
 ALTER TABLE order_src RENAME TO order_src_95865504ddaf4c15ada73081e74117b7;
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
TBLPROPERTIES (
'hmsMirror_Metadata_Stage1'='2023-04-26 12:10:09',
'bucketing_version'='2');
FROM order_src_95865504ddaf4c15ada73081e74117b7 INSERT OVERWRITE TABLE order_src SELECT *;
