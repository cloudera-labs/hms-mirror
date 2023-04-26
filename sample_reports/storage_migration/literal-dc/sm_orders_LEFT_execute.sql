-- EXECUTION script for sm_orders on LEFT cluster

-- 2023-04-26_12-05-53-- These are the command run on the LEFT cluster when `-e` is used.
-- Alter Database Location
ALTER DATABASE sm_orders SET LOCATION "ofs://OHOME90/warehouse/tablespace/external/hive/sm_orders.db";
-- Alter Database Managed Location
ALTER DATABASE sm_orders SET MANAGEDLOCATION "ofs://OHOME90/warehouse/tablespace/managed/hive/sm_orders.db";

--    Table: order_item_orc
USE sm_orders;
ALTER TABLE order_item_orc UNSET TBLPROPERTIES ("TRANSLATED_TO_EXTERNAL");
 ALTER TABLE order_item_orc RENAME TO order_item_orc_b1af29dcb3d84091a597161c999046c6;
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
'hmsMirror_Metadata_Stage1'='2023-04-26 12:05:53',
'TRANSLATED_TO_EXTERNAL'='TRUE',
'bucketing_version'='2',
'external.table.purge'='false'
);

--    Table: order_item_small_orc
USE sm_orders;
 ALTER TABLE order_item_small_orc RENAME TO order_item_small_orc_ac3a68adf0304b2e8614ecedd119c20c;
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
'hmsMirror_Metadata_Stage1'='2023-04-26 12:05:53',
'bucketing_version'='2');

--    Table: order_orc
USE sm_orders;
 ALTER TABLE order_orc RENAME TO order_orc_2d560d516fa84983a96970e252f6a4b0;
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
'hmsMirror_Metadata_Stage1'='2023-04-26 12:05:53',
'bucketing_version'='2');

--    Table: order_small_orc
USE sm_orders;
 ALTER TABLE order_small_orc RENAME TO order_small_orc_14fa588056ea488c9804b760550a747e;
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
'hmsMirror_Metadata_Stage1'='2023-04-26 12:05:53',
'bucketing_version'='2');

--    Table: order_src
USE sm_orders;
 ALTER TABLE order_src RENAME TO order_src_d5a6b71c3287401e80b88abbf3ee44f3;
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
'hmsMirror_Metadata_Stage1'='2023-04-26 12:05:53',
'bucketing_version'='2');
