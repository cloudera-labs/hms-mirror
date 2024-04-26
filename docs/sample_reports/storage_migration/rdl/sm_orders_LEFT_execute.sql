-- EXECUTION script for sm_orders on LEFT cluster

-- 2023-04-26_12-09-32-- These are the command run on the LEFT cluster when `-e` is used.
-- Alter Database Location
ALTER DATABASE sm_orders SET LOCATION "ofs://OHOME90/finance/operations-ext/sm_orders.db";
-- Alter Database Managed Location
ALTER DATABASE sm_orders SET MANAGEDLOCATION "ofs://OHOME90/finance/operations-mngd/sm_orders.db";

--    Table: mngd_order_item_orc
USE sm_orders;
 ALTER TABLE mngd_order_item_orc RENAME TO mngd_order_item_orc_bc9c89a2fc7f4fd2a64402869b97ca42;
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
'hmsMirror_Metadata_Stage1'='2023-04-26 12:09:32',
'bucketing_version'='2',
'transactional'='true',
'transactional_properties'='insert_only'
);
FROM mngd_order_item_orc_bc9c89a2fc7f4fd2a64402869b97ca42 INSERT OVERWRITE TABLE mngd_order_item_orc SELECT *;

--    Table: mngd_order_item_small_orc
USE sm_orders;
 ALTER TABLE mngd_order_item_small_orc RENAME TO mngd_order_item_small_orc_b028df3715c541d784dda9deb29936c3;
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
'hmsMirror_Metadata_Stage1'='2023-04-26 12:09:32',
'bucketing_version'='2',
'transactional'='true',
'transactional_properties'='insert_only'
);
FROM mngd_order_item_small_orc_b028df3715c541d784dda9deb29936c3 INSERT OVERWRITE TABLE mngd_order_item_small_orc SELECT *;

--    Table: mngd_order_orc
USE sm_orders;
 ALTER TABLE mngd_order_orc RENAME TO mngd_order_orc_da7c308560bd4d0a8790d1f7b68b64cc;
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
'hmsMirror_Metadata_Stage1'='2023-04-26 12:09:32',
'bucketing_version'='2',
'transactional'='true',
'transactional_properties'='insert_only'
);
FROM mngd_order_orc_da7c308560bd4d0a8790d1f7b68b64cc INSERT OVERWRITE TABLE mngd_order_orc SELECT *;

--    Table: mngd_order_small_orc
USE sm_orders;
 ALTER TABLE mngd_order_small_orc RENAME TO mngd_order_small_orc_640778cfae4e4b9182db8d4904ca936a;
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
'hmsMirror_Metadata_Stage1'='2023-04-26 12:09:32',
'bucketing_version'='2',
'transactional'='true',
'transactional_properties'='insert_only'
);
FROM mngd_order_small_orc_640778cfae4e4b9182db8d4904ca936a INSERT OVERWRITE TABLE mngd_order_small_orc SELECT *;

--    Table: order_item_orc
USE sm_orders;
ALTER TABLE order_item_orc UNSET TBLPROPERTIES ("TRANSLATED_TO_EXTERNAL");
 ALTER TABLE order_item_orc RENAME TO order_item_orc_e2b738365a514daa94500b657cc1350c;
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
'hmsMirror_Metadata_Stage1'='2023-04-26 12:09:32',
'TRANSLATED_TO_EXTERNAL'='TRUE',
'bucketing_version'='2',
'external.table.purge'='false'
);
FROM order_item_orc_e2b738365a514daa94500b657cc1350c INSERT OVERWRITE TABLE order_item_orc SELECT *;

--    Table: order_item_small_orc
USE sm_orders;
 ALTER TABLE order_item_small_orc RENAME TO order_item_small_orc_ced67c4c3fbc498b8018c39b5b567068;
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
'hmsMirror_Metadata_Stage1'='2023-04-26 12:09:32',
'bucketing_version'='2');
FROM order_item_small_orc_ced67c4c3fbc498b8018c39b5b567068 INSERT OVERWRITE TABLE order_item_small_orc SELECT *;

--    Table: order_orc
USE sm_orders;
 ALTER TABLE order_orc RENAME TO order_orc_6e69c0d258c14399ac65323ca435fed5;
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
'hmsMirror_Metadata_Stage1'='2023-04-26 12:09:32',
'bucketing_version'='2');
FROM order_orc_6e69c0d258c14399ac65323ca435fed5 INSERT OVERWRITE TABLE order_orc SELECT *;

--    Table: order_small_orc
USE sm_orders;
 ALTER TABLE order_small_orc RENAME TO order_small_orc_4fdbbb7e60644e15b5cc690910d7457f;
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
'hmsMirror_Metadata_Stage1'='2023-04-26 12:09:32',
'bucketing_version'='2');
FROM order_small_orc_4fdbbb7e60644e15b5cc690910d7457f INSERT OVERWRITE TABLE order_small_orc SELECT *;

--    Table: order_src
USE sm_orders;
 ALTER TABLE order_src RENAME TO order_src_869b10b9a09e49068dd418509f4febca;
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
'hmsMirror_Metadata_Stage1'='2023-04-26 12:09:32',
'bucketing_version'='2');
FROM order_src_869b10b9a09e49068dd418509f4febca INSERT OVERWRITE TABLE order_src SELECT *;
