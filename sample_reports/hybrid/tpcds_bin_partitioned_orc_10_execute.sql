-- Copyright 2021 Cloudera, Inc. All Rights Reserved.
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--       http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

-- EXECUTION script for RIGHT cluster

-- 2021-05-11_18-41-13-- These are the command run on the RIGHT cluster when `-e` is used.
--    Table: call_center
USE tpcds_bin_partitioned_orc_10;
CREATE EXTERNAL TABLE `call_center`(
  `cc_call_center_sk` bigint, 
  `cc_call_center_id` char(16), 
  `cc_rec_start_date` date, 
  `cc_rec_end_date` date, 
  `cc_closed_date_sk` bigint, 
  `cc_open_date_sk` bigint, 
  `cc_name` varchar(50), 
  `cc_class` varchar(50), 
  `cc_employees` int, 
  `cc_sq_ft` int, 
  `cc_hours` char(20), 
  `cc_manager` varchar(40), 
  `cc_mkt_id` int, 
  `cc_mkt_class` char(50), 
  `cc_mkt_desc` varchar(100), 
  `cc_market_manager` varchar(40), 
  `cc_division` int, 
  `cc_division_name` varchar(50), 
  `cc_company` int, 
  `cc_company_name` char(50), 
  `cc_street_number` char(10), 
  `cc_street_name` varchar(60), 
  `cc_street_type` char(15), 
  `cc_suite_number` char(10), 
  `cc_city` varchar(60), 
  `cc_county` varchar(30), 
  `cc_state` char(2), 
  `cc_zip` char(10), 
  `cc_country` varchar(20), 
  `cc_gmt_offset` decimal(5,2), 
  `cc_tax_percentage` decimal(5,2))
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
  'hdfs://HDP50/apps/hive/warehouse/tpcds_bin_partitioned_orc_10.db/call_center'
TBLPROPERTIES (
'hmsMirror_Metadata_Stage1'='2021-05-11 18:41:12',
'hmsMirror_Converted'='true',
'discover.partitions'='true',
'hmsMirror_LegacyManaged'='true',
  'transient_lastDdlTime'='1609880768');
USE tpcds_bin_partitioned_orc_10;
DROP TABLE IF EXISTS tpcds_bin_partitioned_orc_10.transfer_call_center;
CREATE EXTERNAL TABLE `transfer_call_center`(
  `cc_call_center_sk` bigint, 
  `cc_call_center_id` char(16), 
  `cc_rec_start_date` date, 
  `cc_rec_end_date` date, 
  `cc_closed_date_sk` bigint, 
  `cc_open_date_sk` bigint, 
  `cc_name` varchar(50), 
  `cc_class` varchar(50), 
  `cc_employees` int, 
  `cc_sq_ft` int, 
  `cc_hours` char(20), 
  `cc_manager` varchar(40), 
  `cc_mkt_id` int, 
  `cc_mkt_class` char(50), 
  `cc_mkt_desc` varchar(100), 
  `cc_market_manager` varchar(40), 
  `cc_division` int, 
  `cc_division_name` varchar(50), 
  `cc_company` int, 
  `cc_company_name` char(50), 
  `cc_street_number` char(10), 
  `cc_street_name` varchar(60), 
  `cc_street_type` char(15), 
  `cc_suite_number` char(10), 
  `cc_city` varchar(60), 
  `cc_county` varchar(30), 
  `cc_state` char(2), 
  `cc_zip` char(10), 
  `cc_country` varchar(20), 
  `cc_gmt_offset` decimal(5,2), 
  `cc_tax_percentage` decimal(5,2))
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
'hdfs://HOME90/apps/hive/warehouse/tpcds_bin_partitioned_orc_10.db/call_center'
TBLPROPERTIES (
'hmsMirror_Metadata_Stage1'='2021-05-11 18:41:13',
'hmsMirror_LegacyManaged'='true',
'hmsMirror_Converted'='true',
'external.table.purge'='true',
'discover.partitions'='true',
  'transient_lastDdlTime'='1609880768');
USE tpcds_bin_partitioned_orc_10;
FROM call_center INSERT OVERWRITE TABLE transfer_call_center SELECT *;
USE tpcds_bin_partitioned_orc_10;
DROP TABLE IF EXISTS tpcds_bin_partitioned_orc_10.call_center;
 ALTER TABLE transfer_call_center RENAME TO call_center;
--    Table: catalog_page
USE tpcds_bin_partitioned_orc_10;
CREATE EXTERNAL TABLE `catalog_page`(
  `cp_catalog_page_sk` bigint, 
  `cp_catalog_page_id` char(16), 
  `cp_start_date_sk` bigint, 
  `cp_end_date_sk` bigint, 
  `cp_department` varchar(50), 
  `cp_catalog_number` int, 
  `cp_catalog_page_number` int, 
  `cp_description` varchar(100), 
  `cp_type` varchar(100))
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
  'hdfs://HDP50/apps/hive/warehouse/tpcds_bin_partitioned_orc_10.db/catalog_page'
TBLPROPERTIES (
'hmsMirror_Metadata_Stage1'='2021-05-11 18:41:12',
'hmsMirror_Converted'='true',
'discover.partitions'='true',
'hmsMirror_LegacyManaged'='true',
  'transient_lastDdlTime'='1609880806');
USE tpcds_bin_partitioned_orc_10;
DROP TABLE IF EXISTS tpcds_bin_partitioned_orc_10.transfer_catalog_page;
CREATE EXTERNAL TABLE `transfer_catalog_page`(
  `cp_catalog_page_sk` bigint, 
  `cp_catalog_page_id` char(16), 
  `cp_start_date_sk` bigint, 
  `cp_end_date_sk` bigint, 
  `cp_department` varchar(50), 
  `cp_catalog_number` int, 
  `cp_catalog_page_number` int, 
  `cp_description` varchar(100), 
  `cp_type` varchar(100))
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
'hdfs://HOME90/apps/hive/warehouse/tpcds_bin_partitioned_orc_10.db/catalog_page'
TBLPROPERTIES (
'hmsMirror_Metadata_Stage1'='2021-05-11 18:41:12',
'hmsMirror_LegacyManaged'='true',
'hmsMirror_Converted'='true',
'external.table.purge'='true',
'discover.partitions'='true',
  'transient_lastDdlTime'='1609880806');
USE tpcds_bin_partitioned_orc_10;
FROM catalog_page INSERT OVERWRITE TABLE transfer_catalog_page SELECT *;
USE tpcds_bin_partitioned_orc_10;
DROP TABLE IF EXISTS tpcds_bin_partitioned_orc_10.catalog_page;
 ALTER TABLE transfer_catalog_page RENAME TO catalog_page;
--    Table: catalog_returns
EXPORT TABLE tpcds_bin_partitioned_orc_10.catalog_returns TO "hdfs://HDP50/apps/hive/warehouse/export_tpcds_bin_partitioned_orc_10/catalog_returns";
USE tpcds_bin_partitioned_orc_10;
IMPORT EXTERNAL TABLE tpcds_bin_partitioned_orc_10.catalog_returns FROM "hdfs://HDP50/apps/hive/warehouse/export_tpcds_bin_partitioned_orc_10/catalog_returns" LOCATION "hdfs://HOME90/apps/hive/warehouse/tpcds_bin_partitioned_orc_10.db/catalog_returns";
ALTER TABLE tpcds_bin_partitioned_orc_10.catalog_returns SET TBLPROPERTIES ("external.table.purge"="true");
ALTER TABLE tpcds_bin_partitioned_orc_10.catalog_returns SET TBLPROPERTIES ("discover.partitions"="true");
ALTER TABLE tpcds_bin_partitioned_orc_10.catalog_returns SET TBLPROPERTIES ("hmsMirror_Storage_IMPORT_Stage2"="2021-05-11 18:41:13");
ALTER TABLE tpcds_bin_partitioned_orc_10.catalog_returns SET TBLPROPERTIES ("hmsMirror_Converted"="true");
--    Table: catalog_sales
EXPORT TABLE tpcds_bin_partitioned_orc_10.catalog_sales TO "hdfs://HDP50/apps/hive/warehouse/export_tpcds_bin_partitioned_orc_10/catalog_sales";
USE tpcds_bin_partitioned_orc_10;
IMPORT EXTERNAL TABLE tpcds_bin_partitioned_orc_10.catalog_sales FROM "hdfs://HDP50/apps/hive/warehouse/export_tpcds_bin_partitioned_orc_10/catalog_sales" LOCATION "hdfs://HOME90/apps/hive/warehouse/tpcds_bin_partitioned_orc_10.db/catalog_sales";
ALTER TABLE tpcds_bin_partitioned_orc_10.catalog_sales SET TBLPROPERTIES ("external.table.purge"="true");
ALTER TABLE tpcds_bin_partitioned_orc_10.catalog_sales SET TBLPROPERTIES ("discover.partitions"="true");
ALTER TABLE tpcds_bin_partitioned_orc_10.catalog_sales SET TBLPROPERTIES ("hmsMirror_Storage_IMPORT_Stage2"="2021-05-11 18:41:13");
ALTER TABLE tpcds_bin_partitioned_orc_10.catalog_sales SET TBLPROPERTIES ("hmsMirror_Converted"="true");
--    Table: customer
USE tpcds_bin_partitioned_orc_10;
CREATE EXTERNAL TABLE `customer`(
  `c_customer_sk` bigint, 
  `c_customer_id` char(16), 
  `c_current_cdemo_sk` bigint, 
  `c_current_hdemo_sk` bigint, 
  `c_current_addr_sk` bigint, 
  `c_first_shipto_date_sk` bigint, 
  `c_first_sales_date_sk` bigint, 
  `c_salutation` char(10), 
  `c_first_name` char(20), 
  `c_last_name` char(30), 
  `c_preferred_cust_flag` char(1), 
  `c_birth_day` int, 
  `c_birth_month` int, 
  `c_birth_year` int, 
  `c_birth_country` varchar(20), 
  `c_login` char(13), 
  `c_email_address` char(50), 
  `c_last_review_date_sk` bigint)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
  'hdfs://HDP50/apps/hive/warehouse/tpcds_bin_partitioned_orc_10.db/customer'
TBLPROPERTIES (
'hmsMirror_Metadata_Stage1'='2021-05-11 18:41:13',
'hmsMirror_Converted'='true',
'discover.partitions'='true',
'hmsMirror_LegacyManaged'='true',
  'transient_lastDdlTime'='1609880557');
USE tpcds_bin_partitioned_orc_10;
DROP TABLE IF EXISTS tpcds_bin_partitioned_orc_10.transfer_customer;
CREATE EXTERNAL TABLE `transfer_customer`(
  `c_customer_sk` bigint, 
  `c_customer_id` char(16), 
  `c_current_cdemo_sk` bigint, 
  `c_current_hdemo_sk` bigint, 
  `c_current_addr_sk` bigint, 
  `c_first_shipto_date_sk` bigint, 
  `c_first_sales_date_sk` bigint, 
  `c_salutation` char(10), 
  `c_first_name` char(20), 
  `c_last_name` char(30), 
  `c_preferred_cust_flag` char(1), 
  `c_birth_day` int, 
  `c_birth_month` int, 
  `c_birth_year` int, 
  `c_birth_country` varchar(20), 
  `c_login` char(13), 
  `c_email_address` char(50), 
  `c_last_review_date_sk` bigint)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
'hdfs://HOME90/apps/hive/warehouse/tpcds_bin_partitioned_orc_10.db/customer'
TBLPROPERTIES (
'hmsMirror_Metadata_Stage1'='2021-05-11 18:41:13',
'hmsMirror_LegacyManaged'='true',
'hmsMirror_Converted'='true',
'external.table.purge'='true',
'discover.partitions'='true',
  'transient_lastDdlTime'='1609880557');
USE tpcds_bin_partitioned_orc_10;
FROM customer INSERT OVERWRITE TABLE transfer_customer SELECT *;
USE tpcds_bin_partitioned_orc_10;
DROP TABLE IF EXISTS tpcds_bin_partitioned_orc_10.customer;
 ALTER TABLE transfer_customer RENAME TO customer;
--    Table: customer_address
USE tpcds_bin_partitioned_orc_10;
CREATE EXTERNAL TABLE `customer_address`(
  `ca_address_sk` bigint, 
  `ca_address_id` char(16), 
  `ca_street_number` char(10), 
  `ca_street_name` varchar(60), 
  `ca_street_type` char(15), 
  `ca_suite_number` char(10), 
  `ca_city` varchar(60), 
  `ca_county` varchar(30), 
  `ca_state` char(2), 
  `ca_zip` char(10), 
  `ca_country` varchar(20), 
  `ca_gmt_offset` decimal(5,2), 
  `ca_location_type` char(20))
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
  'hdfs://HDP50/apps/hive/warehouse/tpcds_bin_partitioned_orc_10.db/customer_address'
TBLPROPERTIES (
'hmsMirror_Metadata_Stage1'='2021-05-11 18:41:13',
'hmsMirror_Converted'='true',
'discover.partitions'='true',
'hmsMirror_LegacyManaged'='true',
  'transient_lastDdlTime'='1609880632');
USE tpcds_bin_partitioned_orc_10;
DROP TABLE IF EXISTS tpcds_bin_partitioned_orc_10.transfer_customer_address;
CREATE EXTERNAL TABLE `transfer_customer_address`(
  `ca_address_sk` bigint, 
  `ca_address_id` char(16), 
  `ca_street_number` char(10), 
  `ca_street_name` varchar(60), 
  `ca_street_type` char(15), 
  `ca_suite_number` char(10), 
  `ca_city` varchar(60), 
  `ca_county` varchar(30), 
  `ca_state` char(2), 
  `ca_zip` char(10), 
  `ca_country` varchar(20), 
  `ca_gmt_offset` decimal(5,2), 
  `ca_location_type` char(20))
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
'hdfs://HOME90/apps/hive/warehouse/tpcds_bin_partitioned_orc_10.db/customer_address'
TBLPROPERTIES (
'hmsMirror_Metadata_Stage1'='2021-05-11 18:41:13',
'hmsMirror_LegacyManaged'='true',
'hmsMirror_Converted'='true',
'external.table.purge'='true',
'discover.partitions'='true',
  'transient_lastDdlTime'='1609880632');
USE tpcds_bin_partitioned_orc_10;
FROM customer_address INSERT OVERWRITE TABLE transfer_customer_address SELECT *;
USE tpcds_bin_partitioned_orc_10;
DROP TABLE IF EXISTS tpcds_bin_partitioned_orc_10.customer_address;
 ALTER TABLE transfer_customer_address RENAME TO customer_address;
--    Table: customer_demographics
USE tpcds_bin_partitioned_orc_10;
CREATE EXTERNAL TABLE `customer_demographics`(
  `cd_demo_sk` bigint, 
  `cd_gender` char(1), 
  `cd_marital_status` char(1), 
  `cd_education_status` char(20), 
  `cd_purchase_estimate` int, 
  `cd_credit_rating` char(10), 
  `cd_dep_count` int, 
  `cd_dep_employed_count` int, 
  `cd_dep_college_count` int)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
  'hdfs://HDP50/apps/hive/warehouse/tpcds_bin_partitioned_orc_10.db/customer_demographics'
TBLPROPERTIES (
'hmsMirror_Metadata_Stage1'='2021-05-11 18:41:13',
'hmsMirror_Converted'='true',
'discover.partitions'='true',
'hmsMirror_LegacyManaged'='true',
  'transient_lastDdlTime'='1609880581');
USE tpcds_bin_partitioned_orc_10;
DROP TABLE IF EXISTS tpcds_bin_partitioned_orc_10.transfer_customer_demographics;
CREATE EXTERNAL TABLE `transfer_customer_demographics`(
  `cd_demo_sk` bigint, 
  `cd_gender` char(1), 
  `cd_marital_status` char(1), 
  `cd_education_status` char(20), 
  `cd_purchase_estimate` int, 
  `cd_credit_rating` char(10), 
  `cd_dep_count` int, 
  `cd_dep_employed_count` int, 
  `cd_dep_college_count` int)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
'hdfs://HOME90/apps/hive/warehouse/tpcds_bin_partitioned_orc_10.db/customer_demographics'
TBLPROPERTIES (
'hmsMirror_Metadata_Stage1'='2021-05-11 18:41:13',
'hmsMirror_LegacyManaged'='true',
'hmsMirror_Converted'='true',
'external.table.purge'='true',
'discover.partitions'='true',
  'transient_lastDdlTime'='1609880581');
USE tpcds_bin_partitioned_orc_10;
FROM customer_demographics INSERT OVERWRITE TABLE transfer_customer_demographics SELECT *;
USE tpcds_bin_partitioned_orc_10;
DROP TABLE IF EXISTS tpcds_bin_partitioned_orc_10.customer_demographics;
 ALTER TABLE transfer_customer_demographics RENAME TO customer_demographics;
--    Table: date_dim
USE tpcds_bin_partitioned_orc_10;
CREATE EXTERNAL TABLE `date_dim`(
  `d_date_sk` bigint, 
  `d_date_id` char(16), 
  `d_date` date, 
  `d_month_seq` int, 
  `d_week_seq` int, 
  `d_quarter_seq` int, 
  `d_year` int, 
  `d_dow` int, 
  `d_moy` int, 
  `d_dom` int, 
  `d_qoy` int, 
  `d_fy_year` int, 
  `d_fy_quarter_seq` int, 
  `d_fy_week_seq` int, 
  `d_day_name` char(9), 
  `d_quarter_name` char(6), 
  `d_holiday` char(1), 
  `d_weekend` char(1), 
  `d_following_holiday` char(1), 
  `d_first_dom` int, 
  `d_last_dom` int, 
  `d_same_day_ly` int, 
  `d_same_day_lq` int, 
  `d_current_day` char(1), 
  `d_current_week` char(1), 
  `d_current_month` char(1), 
  `d_current_quarter` char(1), 
  `d_current_year` char(1))
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
  'hdfs://HDP50/apps/hive/warehouse/tpcds_bin_partitioned_orc_10.db/date_dim'
TBLPROPERTIES (
'hmsMirror_Metadata_Stage1'='2021-05-11 18:41:13',
'hmsMirror_Converted'='true',
'discover.partitions'='true',
'hmsMirror_LegacyManaged'='true',
  'transient_lastDdlTime'='1609880485');
USE tpcds_bin_partitioned_orc_10;
DROP TABLE IF EXISTS tpcds_bin_partitioned_orc_10.transfer_date_dim;
CREATE EXTERNAL TABLE `transfer_date_dim`(
  `d_date_sk` bigint, 
  `d_date_id` char(16), 
  `d_date` date, 
  `d_month_seq` int, 
  `d_week_seq` int, 
  `d_quarter_seq` int, 
  `d_year` int, 
  `d_dow` int, 
  `d_moy` int, 
  `d_dom` int, 
  `d_qoy` int, 
  `d_fy_year` int, 
  `d_fy_quarter_seq` int, 
  `d_fy_week_seq` int, 
  `d_day_name` char(9), 
  `d_quarter_name` char(6), 
  `d_holiday` char(1), 
  `d_weekend` char(1), 
  `d_following_holiday` char(1), 
  `d_first_dom` int, 
  `d_last_dom` int, 
  `d_same_day_ly` int, 
  `d_same_day_lq` int, 
  `d_current_day` char(1), 
  `d_current_week` char(1), 
  `d_current_month` char(1), 
  `d_current_quarter` char(1), 
  `d_current_year` char(1))
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
'hdfs://HOME90/apps/hive/warehouse/tpcds_bin_partitioned_orc_10.db/date_dim'
TBLPROPERTIES (
'hmsMirror_Metadata_Stage1'='2021-05-11 18:41:13',
'hmsMirror_LegacyManaged'='true',
'hmsMirror_Converted'='true',
'external.table.purge'='true',
'discover.partitions'='true',
  'transient_lastDdlTime'='1609880485');
USE tpcds_bin_partitioned_orc_10;
FROM date_dim INSERT OVERWRITE TABLE transfer_date_dim SELECT *;
USE tpcds_bin_partitioned_orc_10;
DROP TABLE IF EXISTS tpcds_bin_partitioned_orc_10.date_dim;
 ALTER TABLE transfer_date_dim RENAME TO date_dim;
--    Table: household_demographics
USE tpcds_bin_partitioned_orc_10;
CREATE EXTERNAL TABLE `household_demographics`(
  `hd_demo_sk` bigint, 
  `hd_income_band_sk` bigint, 
  `hd_buy_potential` char(15), 
  `hd_dep_count` int, 
  `hd_vehicle_count` int)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
  'hdfs://HDP50/apps/hive/warehouse/tpcds_bin_partitioned_orc_10.db/household_demographics'
TBLPROPERTIES (
'hmsMirror_Metadata_Stage1'='2021-05-11 18:41:13',
'hmsMirror_Converted'='true',
'discover.partitions'='true',
'hmsMirror_LegacyManaged'='true',
  'transient_lastDdlTime'='1609880604');
USE tpcds_bin_partitioned_orc_10;
DROP TABLE IF EXISTS tpcds_bin_partitioned_orc_10.transfer_household_demographics;
CREATE EXTERNAL TABLE `transfer_household_demographics`(
  `hd_demo_sk` bigint, 
  `hd_income_band_sk` bigint, 
  `hd_buy_potential` char(15), 
  `hd_dep_count` int, 
  `hd_vehicle_count` int)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
'hdfs://HOME90/apps/hive/warehouse/tpcds_bin_partitioned_orc_10.db/household_demographics'
TBLPROPERTIES (
'hmsMirror_Metadata_Stage1'='2021-05-11 18:41:13',
'hmsMirror_LegacyManaged'='true',
'hmsMirror_Converted'='true',
'external.table.purge'='true',
'discover.partitions'='true',
  'transient_lastDdlTime'='1609880604');
USE tpcds_bin_partitioned_orc_10;
FROM household_demographics INSERT OVERWRITE TABLE transfer_household_demographics SELECT *;
USE tpcds_bin_partitioned_orc_10;
DROP TABLE IF EXISTS tpcds_bin_partitioned_orc_10.household_demographics;
 ALTER TABLE transfer_household_demographics RENAME TO household_demographics;
--    Table: income_band
USE tpcds_bin_partitioned_orc_10;
CREATE EXTERNAL TABLE `income_band`(
  `ib_income_band_sk` bigint, 
  `ib_lower_bound` int, 
  `ib_upper_bound` int)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
  'hdfs://HDP50/apps/hive/warehouse/tpcds_bin_partitioned_orc_10.db/income_band'
TBLPROPERTIES (
'hmsMirror_Metadata_Stage1'='2021-05-11 18:41:13',
'hmsMirror_Converted'='true',
'discover.partitions'='true',
'hmsMirror_LegacyManaged'='true',
  'transient_lastDdlTime'='1609880750');
USE tpcds_bin_partitioned_orc_10;
DROP TABLE IF EXISTS tpcds_bin_partitioned_orc_10.transfer_income_band;
CREATE EXTERNAL TABLE `transfer_income_band`(
  `ib_income_band_sk` bigint, 
  `ib_lower_bound` int, 
  `ib_upper_bound` int)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
'hdfs://HOME90/apps/hive/warehouse/tpcds_bin_partitioned_orc_10.db/income_band'
TBLPROPERTIES (
'hmsMirror_Metadata_Stage1'='2021-05-11 18:41:13',
'hmsMirror_LegacyManaged'='true',
'hmsMirror_Converted'='true',
'external.table.purge'='true',
'discover.partitions'='true',
  'transient_lastDdlTime'='1609880750');
USE tpcds_bin_partitioned_orc_10;
FROM income_band INSERT OVERWRITE TABLE transfer_income_band SELECT *;
USE tpcds_bin_partitioned_orc_10;
DROP TABLE IF EXISTS tpcds_bin_partitioned_orc_10.income_band;
 ALTER TABLE transfer_income_band RENAME TO income_band;
--    Table: inventory
USE tpcds_bin_partitioned_orc_10;
CREATE EXTERNAL TABLE `inventory`(
  `inv_date_sk` bigint, 
  `inv_item_sk` bigint, 
  `inv_warehouse_sk` bigint, 
  `inv_quantity_on_hand` int)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
  'hdfs://HDP50/apps/hive/warehouse/tpcds_bin_partitioned_orc_10.db/inventory'
TBLPROPERTIES (
'hmsMirror_Metadata_Stage1'='2021-05-11 18:41:13',
'hmsMirror_Converted'='true',
'discover.partitions'='true',
'hmsMirror_LegacyManaged'='true',
  'transient_lastDdlTime'='1609886025');
USE tpcds_bin_partitioned_orc_10;
DROP TABLE IF EXISTS tpcds_bin_partitioned_orc_10.transfer_inventory;
CREATE EXTERNAL TABLE `transfer_inventory`(
  `inv_date_sk` bigint, 
  `inv_item_sk` bigint, 
  `inv_warehouse_sk` bigint, 
  `inv_quantity_on_hand` int)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
'hdfs://HOME90/apps/hive/warehouse/tpcds_bin_partitioned_orc_10.db/inventory'
TBLPROPERTIES (
'hmsMirror_Metadata_Stage1'='2021-05-11 18:41:13',
'hmsMirror_LegacyManaged'='true',
'hmsMirror_Converted'='true',
'external.table.purge'='true',
'discover.partitions'='true',
  'transient_lastDdlTime'='1609886025');
USE tpcds_bin_partitioned_orc_10;
FROM inventory INSERT OVERWRITE TABLE transfer_inventory SELECT *;
USE tpcds_bin_partitioned_orc_10;
DROP TABLE IF EXISTS tpcds_bin_partitioned_orc_10.inventory;
 ALTER TABLE transfer_inventory RENAME TO inventory;
--    Table: item
USE tpcds_bin_partitioned_orc_10;
CREATE EXTERNAL TABLE `item`(
  `i_item_sk` bigint, 
  `i_item_id` char(16), 
  `i_rec_start_date` date, 
  `i_rec_end_date` date, 
  `i_item_desc` varchar(200), 
  `i_current_price` decimal(7,2), 
  `i_wholesale_cost` decimal(7,2), 
  `i_brand_id` int, 
  `i_brand` char(50), 
  `i_class_id` int, 
  `i_class` char(50), 
  `i_category_id` int, 
  `i_category` char(50), 
  `i_manufact_id` int, 
  `i_manufact` char(50), 
  `i_size` char(20), 
  `i_formulation` char(20), 
  `i_color` char(20), 
  `i_units` char(10), 
  `i_container` char(10), 
  `i_manager_id` int, 
  `i_product_name` char(50))
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
  'hdfs://HDP50/apps/hive/warehouse/tpcds_bin_partitioned_orc_10.db/item'
TBLPROPERTIES (
'hmsMirror_Metadata_Stage1'='2021-05-11 18:41:13',
'hmsMirror_Converted'='true',
'discover.partitions'='true',
'hmsMirror_LegacyManaged'='true',
  'transient_lastDdlTime'='1609880532');
USE tpcds_bin_partitioned_orc_10;
DROP TABLE IF EXISTS tpcds_bin_partitioned_orc_10.transfer_item;
CREATE EXTERNAL TABLE `transfer_item`(
  `i_item_sk` bigint, 
  `i_item_id` char(16), 
  `i_rec_start_date` date, 
  `i_rec_end_date` date, 
  `i_item_desc` varchar(200), 
  `i_current_price` decimal(7,2), 
  `i_wholesale_cost` decimal(7,2), 
  `i_brand_id` int, 
  `i_brand` char(50), 
  `i_class_id` int, 
  `i_class` char(50), 
  `i_category_id` int, 
  `i_category` char(50), 
  `i_manufact_id` int, 
  `i_manufact` char(50), 
  `i_size` char(20), 
  `i_formulation` char(20), 
  `i_color` char(20), 
  `i_units` char(10), 
  `i_container` char(10), 
  `i_manager_id` int, 
  `i_product_name` char(50))
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
'hdfs://HOME90/apps/hive/warehouse/tpcds_bin_partitioned_orc_10.db/item'
TBLPROPERTIES (
'hmsMirror_Metadata_Stage1'='2021-05-11 18:41:13',
'hmsMirror_LegacyManaged'='true',
'hmsMirror_Converted'='true',
'external.table.purge'='true',
'discover.partitions'='true',
  'transient_lastDdlTime'='1609880532');
USE tpcds_bin_partitioned_orc_10;
FROM item INSERT OVERWRITE TABLE transfer_item SELECT *;
USE tpcds_bin_partitioned_orc_10;
DROP TABLE IF EXISTS tpcds_bin_partitioned_orc_10.item;
 ALTER TABLE transfer_item RENAME TO item;
--    Table: promotion
USE tpcds_bin_partitioned_orc_10;
CREATE EXTERNAL TABLE `promotion`(
  `p_promo_sk` bigint, 
  `p_promo_id` char(16), 
  `p_start_date_sk` bigint, 
  `p_end_date_sk` bigint, 
  `p_item_sk` bigint, 
  `p_cost` decimal(15,2), 
  `p_response_target` int, 
  `p_promo_name` char(50), 
  `p_channel_dmail` char(1), 
  `p_channel_email` char(1), 
  `p_channel_catalog` char(1), 
  `p_channel_tv` char(1), 
  `p_channel_radio` char(1), 
  `p_channel_press` char(1), 
  `p_channel_event` char(1), 
  `p_channel_demo` char(1), 
  `p_channel_details` varchar(100), 
  `p_purpose` char(15), 
  `p_discount_active` char(1))
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
  'hdfs://HDP50/apps/hive/warehouse/tpcds_bin_partitioned_orc_10.db/promotion'
TBLPROPERTIES (
'hmsMirror_Metadata_Stage1'='2021-05-11 18:41:13',
'hmsMirror_Converted'='true',
'discover.partitions'='true',
'hmsMirror_LegacyManaged'='true',
  'transient_lastDdlTime'='1609880670');
USE tpcds_bin_partitioned_orc_10;
DROP TABLE IF EXISTS tpcds_bin_partitioned_orc_10.transfer_promotion;
CREATE EXTERNAL TABLE `transfer_promotion`(
  `p_promo_sk` bigint, 
  `p_promo_id` char(16), 
  `p_start_date_sk` bigint, 
  `p_end_date_sk` bigint, 
  `p_item_sk` bigint, 
  `p_cost` decimal(15,2), 
  `p_response_target` int, 
  `p_promo_name` char(50), 
  `p_channel_dmail` char(1), 
  `p_channel_email` char(1), 
  `p_channel_catalog` char(1), 
  `p_channel_tv` char(1), 
  `p_channel_radio` char(1), 
  `p_channel_press` char(1), 
  `p_channel_event` char(1), 
  `p_channel_demo` char(1), 
  `p_channel_details` varchar(100), 
  `p_purpose` char(15), 
  `p_discount_active` char(1))
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
'hdfs://HOME90/apps/hive/warehouse/tpcds_bin_partitioned_orc_10.db/promotion'
TBLPROPERTIES (
'hmsMirror_Metadata_Stage1'='2021-05-11 18:41:13',
'hmsMirror_LegacyManaged'='true',
'hmsMirror_Converted'='true',
'external.table.purge'='true',
'discover.partitions'='true',
  'transient_lastDdlTime'='1609880670');
USE tpcds_bin_partitioned_orc_10;
FROM promotion INSERT OVERWRITE TABLE transfer_promotion SELECT *;
USE tpcds_bin_partitioned_orc_10;
DROP TABLE IF EXISTS tpcds_bin_partitioned_orc_10.promotion;
 ALTER TABLE transfer_promotion RENAME TO promotion;
--    Table: reason
USE tpcds_bin_partitioned_orc_10;
CREATE EXTERNAL TABLE `reason`(
  `r_reason_sk` bigint, 
  `r_reason_id` char(16), 
  `r_reason_desc` char(100))
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
  'hdfs://HDP50/apps/hive/warehouse/tpcds_bin_partitioned_orc_10.db/reason'
TBLPROPERTIES (
'hmsMirror_Metadata_Stage1'='2021-05-11 18:41:13',
'hmsMirror_Converted'='true',
'discover.partitions'='true',
'hmsMirror_LegacyManaged'='true',
  'transient_lastDdlTime'='1609880728');
USE tpcds_bin_partitioned_orc_10;
DROP TABLE IF EXISTS tpcds_bin_partitioned_orc_10.transfer_reason;
CREATE EXTERNAL TABLE `transfer_reason`(
  `r_reason_sk` bigint, 
  `r_reason_id` char(16), 
  `r_reason_desc` char(100))
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
'hdfs://HOME90/apps/hive/warehouse/tpcds_bin_partitioned_orc_10.db/reason'
TBLPROPERTIES (
'hmsMirror_Metadata_Stage1'='2021-05-11 18:41:13',
'hmsMirror_LegacyManaged'='true',
'hmsMirror_Converted'='true',
'external.table.purge'='true',
'discover.partitions'='true',
  'transient_lastDdlTime'='1609880728');
USE tpcds_bin_partitioned_orc_10;
FROM reason INSERT OVERWRITE TABLE transfer_reason SELECT *;
USE tpcds_bin_partitioned_orc_10;
DROP TABLE IF EXISTS tpcds_bin_partitioned_orc_10.reason;
 ALTER TABLE transfer_reason RENAME TO reason;
--    Table: ship_mode
USE tpcds_bin_partitioned_orc_10;
CREATE EXTERNAL TABLE `ship_mode`(
  `sm_ship_mode_sk` bigint, 
  `sm_ship_mode_id` char(16), 
  `sm_type` char(30), 
  `sm_code` char(10), 
  `sm_carrier` char(20), 
  `sm_contract` char(20))
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
  'hdfs://HDP50/apps/hive/warehouse/tpcds_bin_partitioned_orc_10.db/ship_mode'
TBLPROPERTIES (
'hmsMirror_Metadata_Stage1'='2021-05-11 18:41:13',
'hmsMirror_Converted'='true',
'discover.partitions'='true',
'hmsMirror_LegacyManaged'='true',
  'transient_lastDdlTime'='1609880708');
USE tpcds_bin_partitioned_orc_10;
DROP TABLE IF EXISTS tpcds_bin_partitioned_orc_10.transfer_ship_mode;
CREATE EXTERNAL TABLE `transfer_ship_mode`(
  `sm_ship_mode_sk` bigint, 
  `sm_ship_mode_id` char(16), 
  `sm_type` char(30), 
  `sm_code` char(10), 
  `sm_carrier` char(20), 
  `sm_contract` char(20))
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
'hdfs://HOME90/apps/hive/warehouse/tpcds_bin_partitioned_orc_10.db/ship_mode'
TBLPROPERTIES (
'hmsMirror_Metadata_Stage1'='2021-05-11 18:41:13',
'hmsMirror_LegacyManaged'='true',
'hmsMirror_Converted'='true',
'external.table.purge'='true',
'discover.partitions'='true',
  'transient_lastDdlTime'='1609880708');
USE tpcds_bin_partitioned_orc_10;
FROM ship_mode INSERT OVERWRITE TABLE transfer_ship_mode SELECT *;
USE tpcds_bin_partitioned_orc_10;
DROP TABLE IF EXISTS tpcds_bin_partitioned_orc_10.ship_mode;
 ALTER TABLE transfer_ship_mode RENAME TO ship_mode;
--    Table: store
USE tpcds_bin_partitioned_orc_10;
CREATE EXTERNAL TABLE `store`(
  `s_store_sk` bigint, 
  `s_store_id` char(16), 
  `s_rec_start_date` date, 
  `s_rec_end_date` date, 
  `s_closed_date_sk` bigint, 
  `s_store_name` varchar(50), 
  `s_number_employees` int, 
  `s_floor_space` int, 
  `s_hours` char(20), 
  `s_manager` varchar(40), 
  `s_market_id` int, 
  `s_geography_class` varchar(100), 
  `s_market_desc` varchar(100), 
  `s_market_manager` varchar(40), 
  `s_division_id` int, 
  `s_division_name` varchar(50), 
  `s_company_id` int, 
  `s_company_name` varchar(50), 
  `s_street_number` varchar(10), 
  `s_street_name` varchar(60), 
  `s_street_type` char(15), 
  `s_suite_number` char(10), 
  `s_city` varchar(60), 
  `s_county` varchar(30), 
  `s_state` char(2), 
  `s_zip` char(10), 
  `s_country` varchar(20), 
  `s_gmt_offset` decimal(5,2), 
  `s_tax_percentage` decimal(5,2))
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
  'hdfs://HDP50/apps/hive/warehouse/tpcds_bin_partitioned_orc_10.db/store'
TBLPROPERTIES (
'hmsMirror_Metadata_Stage1'='2021-05-11 18:41:13',
'hmsMirror_Converted'='true',
'discover.partitions'='true',
'hmsMirror_LegacyManaged'='true',
  'transient_lastDdlTime'='1609880651');
USE tpcds_bin_partitioned_orc_10;
DROP TABLE IF EXISTS tpcds_bin_partitioned_orc_10.transfer_store;
CREATE EXTERNAL TABLE `transfer_store`(
  `s_store_sk` bigint, 
  `s_store_id` char(16), 
  `s_rec_start_date` date, 
  `s_rec_end_date` date, 
  `s_closed_date_sk` bigint, 
  `s_store_name` varchar(50), 
  `s_number_employees` int, 
  `s_floor_space` int, 
  `s_hours` char(20), 
  `s_manager` varchar(40), 
  `s_market_id` int, 
  `s_geography_class` varchar(100), 
  `s_market_desc` varchar(100), 
  `s_market_manager` varchar(40), 
  `s_division_id` int, 
  `s_division_name` varchar(50), 
  `s_company_id` int, 
  `s_company_name` varchar(50), 
  `s_street_number` varchar(10), 
  `s_street_name` varchar(60), 
  `s_street_type` char(15), 
  `s_suite_number` char(10), 
  `s_city` varchar(60), 
  `s_county` varchar(30), 
  `s_state` char(2), 
  `s_zip` char(10), 
  `s_country` varchar(20), 
  `s_gmt_offset` decimal(5,2), 
  `s_tax_percentage` decimal(5,2))
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
'hdfs://HOME90/apps/hive/warehouse/tpcds_bin_partitioned_orc_10.db/store'
TBLPROPERTIES (
'hmsMirror_Metadata_Stage1'='2021-05-11 18:41:13',
'hmsMirror_LegacyManaged'='true',
'hmsMirror_Converted'='true',
'external.table.purge'='true',
'discover.partitions'='true',
  'transient_lastDdlTime'='1609880651');
USE tpcds_bin_partitioned_orc_10;
FROM store INSERT OVERWRITE TABLE transfer_store SELECT *;
USE tpcds_bin_partitioned_orc_10;
DROP TABLE IF EXISTS tpcds_bin_partitioned_orc_10.store;
 ALTER TABLE transfer_store RENAME TO store;
--    Table: store_returns
EXPORT TABLE tpcds_bin_partitioned_orc_10.store_returns TO "hdfs://HDP50/apps/hive/warehouse/export_tpcds_bin_partitioned_orc_10/store_returns";
USE tpcds_bin_partitioned_orc_10;
IMPORT EXTERNAL TABLE tpcds_bin_partitioned_orc_10.store_returns FROM "hdfs://HDP50/apps/hive/warehouse/export_tpcds_bin_partitioned_orc_10/store_returns" LOCATION "hdfs://HOME90/apps/hive/warehouse/tpcds_bin_partitioned_orc_10.db/store_returns";
ALTER TABLE tpcds_bin_partitioned_orc_10.store_returns SET TBLPROPERTIES ("external.table.purge"="true");
ALTER TABLE tpcds_bin_partitioned_orc_10.store_returns SET TBLPROPERTIES ("discover.partitions"="true");
ALTER TABLE tpcds_bin_partitioned_orc_10.store_returns SET TBLPROPERTIES ("hmsMirror_Storage_IMPORT_Stage2"="2021-05-11 18:41:13");
ALTER TABLE tpcds_bin_partitioned_orc_10.store_returns SET TBLPROPERTIES ("hmsMirror_Converted"="true");
--    Table: store_sales
EXPORT TABLE tpcds_bin_partitioned_orc_10.store_sales TO "hdfs://HDP50/apps/hive/warehouse/export_tpcds_bin_partitioned_orc_10/store_sales";
USE tpcds_bin_partitioned_orc_10;
IMPORT EXTERNAL TABLE tpcds_bin_partitioned_orc_10.store_sales FROM "hdfs://HDP50/apps/hive/warehouse/export_tpcds_bin_partitioned_orc_10/store_sales" LOCATION "hdfs://HOME90/apps/hive/warehouse/tpcds_bin_partitioned_orc_10.db/store_sales";
ALTER TABLE tpcds_bin_partitioned_orc_10.store_sales SET TBLPROPERTIES ("external.table.purge"="true");
ALTER TABLE tpcds_bin_partitioned_orc_10.store_sales SET TBLPROPERTIES ("discover.partitions"="true");
ALTER TABLE tpcds_bin_partitioned_orc_10.store_sales SET TBLPROPERTIES ("hmsMirror_Storage_IMPORT_Stage2"="2021-05-11 18:41:13");
ALTER TABLE tpcds_bin_partitioned_orc_10.store_sales SET TBLPROPERTIES ("hmsMirror_Converted"="true");
--    Table: time_dim
USE tpcds_bin_partitioned_orc_10;
CREATE EXTERNAL TABLE `time_dim`(
  `t_time_sk` bigint, 
  `t_time_id` char(16), 
  `t_time` int, 
  `t_hour` int, 
  `t_minute` int, 
  `t_second` int, 
  `t_am_pm` char(2), 
  `t_shift` char(20), 
  `t_sub_shift` char(20), 
  `t_meal_time` char(20))
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
  'hdfs://HDP50/apps/hive/warehouse/tpcds_bin_partitioned_orc_10.db/time_dim'
TBLPROPERTIES (
'hmsMirror_Metadata_Stage1'='2021-05-11 18:41:13',
'hmsMirror_Converted'='true',
'discover.partitions'='true',
'hmsMirror_LegacyManaged'='true',
  'transient_lastDdlTime'='1609880505');
USE tpcds_bin_partitioned_orc_10;
DROP TABLE IF EXISTS tpcds_bin_partitioned_orc_10.transfer_time_dim;
CREATE EXTERNAL TABLE `transfer_time_dim`(
  `t_time_sk` bigint, 
  `t_time_id` char(16), 
  `t_time` int, 
  `t_hour` int, 
  `t_minute` int, 
  `t_second` int, 
  `t_am_pm` char(2), 
  `t_shift` char(20), 
  `t_sub_shift` char(20), 
  `t_meal_time` char(20))
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
'hdfs://HOME90/apps/hive/warehouse/tpcds_bin_partitioned_orc_10.db/time_dim'
TBLPROPERTIES (
'hmsMirror_Metadata_Stage1'='2021-05-11 18:41:13',
'hmsMirror_LegacyManaged'='true',
'hmsMirror_Converted'='true',
'external.table.purge'='true',
'discover.partitions'='true',
  'transient_lastDdlTime'='1609880505');
USE tpcds_bin_partitioned_orc_10;
FROM time_dim INSERT OVERWRITE TABLE transfer_time_dim SELECT *;
USE tpcds_bin_partitioned_orc_10;
DROP TABLE IF EXISTS tpcds_bin_partitioned_orc_10.time_dim;
 ALTER TABLE transfer_time_dim RENAME TO time_dim;
--    Table: warehouse
USE tpcds_bin_partitioned_orc_10;
CREATE EXTERNAL TABLE `warehouse`(
  `w_warehouse_sk` bigint, 
  `w_warehouse_id` char(16), 
  `w_warehouse_name` varchar(20), 
  `w_warehouse_sq_ft` int, 
  `w_street_number` char(10), 
  `w_street_name` varchar(60), 
  `w_street_type` char(15), 
  `w_suite_number` char(10), 
  `w_city` varchar(60), 
  `w_county` varchar(30), 
  `w_state` char(2), 
  `w_zip` char(10), 
  `w_country` varchar(20), 
  `w_gmt_offset` decimal(5,2))
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
  'hdfs://HDP50/apps/hive/warehouse/tpcds_bin_partitioned_orc_10.db/warehouse'
TBLPROPERTIES (
'hmsMirror_Metadata_Stage1'='2021-05-11 18:41:13',
'hmsMirror_Converted'='true',
'discover.partitions'='true',
'hmsMirror_LegacyManaged'='true',
  'transient_lastDdlTime'='1609880689');
USE tpcds_bin_partitioned_orc_10;
DROP TABLE IF EXISTS tpcds_bin_partitioned_orc_10.transfer_warehouse;
CREATE EXTERNAL TABLE `transfer_warehouse`(
  `w_warehouse_sk` bigint, 
  `w_warehouse_id` char(16), 
  `w_warehouse_name` varchar(20), 
  `w_warehouse_sq_ft` int, 
  `w_street_number` char(10), 
  `w_street_name` varchar(60), 
  `w_street_type` char(15), 
  `w_suite_number` char(10), 
  `w_city` varchar(60), 
  `w_county` varchar(30), 
  `w_state` char(2), 
  `w_zip` char(10), 
  `w_country` varchar(20), 
  `w_gmt_offset` decimal(5,2))
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
'hdfs://HOME90/apps/hive/warehouse/tpcds_bin_partitioned_orc_10.db/warehouse'
TBLPROPERTIES (
'hmsMirror_Metadata_Stage1'='2021-05-11 18:41:13',
'hmsMirror_LegacyManaged'='true',
'hmsMirror_Converted'='true',
'external.table.purge'='true',
'discover.partitions'='true',
  'transient_lastDdlTime'='1609880689');
USE tpcds_bin_partitioned_orc_10;
FROM warehouse INSERT OVERWRITE TABLE transfer_warehouse SELECT *;
USE tpcds_bin_partitioned_orc_10;
DROP TABLE IF EXISTS tpcds_bin_partitioned_orc_10.warehouse;
 ALTER TABLE transfer_warehouse RENAME TO warehouse;
--    Table: web_page
USE tpcds_bin_partitioned_orc_10;
CREATE EXTERNAL TABLE `web_page`(
  `wp_web_page_sk` bigint, 
  `wp_web_page_id` char(16), 
  `wp_rec_start_date` date, 
  `wp_rec_end_date` date, 
  `wp_creation_date_sk` bigint, 
  `wp_access_date_sk` bigint, 
  `wp_autogen_flag` char(1), 
  `wp_customer_sk` bigint, 
  `wp_url` varchar(100), 
  `wp_type` char(50), 
  `wp_char_count` int, 
  `wp_link_count` int, 
  `wp_image_count` int, 
  `wp_max_ad_count` int)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
  'hdfs://HDP50/apps/hive/warehouse/tpcds_bin_partitioned_orc_10.db/web_page'
TBLPROPERTIES (
'hmsMirror_Metadata_Stage1'='2021-05-11 18:41:13',
'hmsMirror_Converted'='true',
'discover.partitions'='true',
'hmsMirror_LegacyManaged'='true',
  'transient_lastDdlTime'='1609880787');
USE tpcds_bin_partitioned_orc_10;
DROP TABLE IF EXISTS tpcds_bin_partitioned_orc_10.transfer_web_page;
CREATE EXTERNAL TABLE `transfer_web_page`(
  `wp_web_page_sk` bigint, 
  `wp_web_page_id` char(16), 
  `wp_rec_start_date` date, 
  `wp_rec_end_date` date, 
  `wp_creation_date_sk` bigint, 
  `wp_access_date_sk` bigint, 
  `wp_autogen_flag` char(1), 
  `wp_customer_sk` bigint, 
  `wp_url` varchar(100), 
  `wp_type` char(50), 
  `wp_char_count` int, 
  `wp_link_count` int, 
  `wp_image_count` int, 
  `wp_max_ad_count` int)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
'hdfs://HOME90/apps/hive/warehouse/tpcds_bin_partitioned_orc_10.db/web_page'
TBLPROPERTIES (
'hmsMirror_Metadata_Stage1'='2021-05-11 18:41:13',
'hmsMirror_LegacyManaged'='true',
'hmsMirror_Converted'='true',
'external.table.purge'='true',
'discover.partitions'='true',
  'transient_lastDdlTime'='1609880787');
USE tpcds_bin_partitioned_orc_10;
FROM web_page INSERT OVERWRITE TABLE transfer_web_page SELECT *;
USE tpcds_bin_partitioned_orc_10;
DROP TABLE IF EXISTS tpcds_bin_partitioned_orc_10.web_page;
 ALTER TABLE transfer_web_page RENAME TO web_page;
--    Table: web_returns
EXPORT TABLE tpcds_bin_partitioned_orc_10.web_returns TO "hdfs://HDP50/apps/hive/warehouse/export_tpcds_bin_partitioned_orc_10/web_returns";
USE tpcds_bin_partitioned_orc_10;
IMPORT EXTERNAL TABLE tpcds_bin_partitioned_orc_10.web_returns FROM "hdfs://HDP50/apps/hive/warehouse/export_tpcds_bin_partitioned_orc_10/web_returns" LOCATION "hdfs://HOME90/apps/hive/warehouse/tpcds_bin_partitioned_orc_10.db/web_returns";
ALTER TABLE tpcds_bin_partitioned_orc_10.web_returns SET TBLPROPERTIES ("external.table.purge"="true");
ALTER TABLE tpcds_bin_partitioned_orc_10.web_returns SET TBLPROPERTIES ("discover.partitions"="true");
ALTER TABLE tpcds_bin_partitioned_orc_10.web_returns SET TBLPROPERTIES ("hmsMirror_Storage_IMPORT_Stage2"="2021-05-11 18:41:13");
ALTER TABLE tpcds_bin_partitioned_orc_10.web_returns SET TBLPROPERTIES ("hmsMirror_Converted"="true");
--    Table: web_sales
EXPORT TABLE tpcds_bin_partitioned_orc_10.web_sales TO "hdfs://HDP50/apps/hive/warehouse/export_tpcds_bin_partitioned_orc_10/web_sales";
USE tpcds_bin_partitioned_orc_10;
IMPORT EXTERNAL TABLE tpcds_bin_partitioned_orc_10.web_sales FROM "hdfs://HDP50/apps/hive/warehouse/export_tpcds_bin_partitioned_orc_10/web_sales" LOCATION "hdfs://HOME90/apps/hive/warehouse/tpcds_bin_partitioned_orc_10.db/web_sales";
ALTER TABLE tpcds_bin_partitioned_orc_10.web_sales SET TBLPROPERTIES ("external.table.purge"="true");
ALTER TABLE tpcds_bin_partitioned_orc_10.web_sales SET TBLPROPERTIES ("discover.partitions"="true");
ALTER TABLE tpcds_bin_partitioned_orc_10.web_sales SET TBLPROPERTIES ("hmsMirror_Storage_IMPORT_Stage2"="2021-05-11 18:41:13");
ALTER TABLE tpcds_bin_partitioned_orc_10.web_sales SET TBLPROPERTIES ("hmsMirror_Converted"="true");
--    Table: web_site
USE tpcds_bin_partitioned_orc_10;
CREATE EXTERNAL TABLE `web_site`(
  `web_site_sk` bigint, 
  `web_site_id` char(16), 
  `web_rec_start_date` date, 
  `web_rec_end_date` date, 
  `web_name` varchar(50), 
  `web_open_date_sk` bigint, 
  `web_close_date_sk` bigint, 
  `web_class` varchar(50), 
  `web_manager` varchar(40), 
  `web_mkt_id` int, 
  `web_mkt_class` varchar(50), 
  `web_mkt_desc` varchar(100), 
  `web_market_manager` varchar(40), 
  `web_company_id` int, 
  `web_company_name` char(50), 
  `web_street_number` char(10), 
  `web_street_name` varchar(60), 
  `web_street_type` char(15), 
  `web_suite_number` char(10), 
  `web_city` varchar(60), 
  `web_county` varchar(30), 
  `web_state` char(2), 
  `web_zip` char(10), 
  `web_country` varchar(20), 
  `web_gmt_offset` decimal(5,2), 
  `web_tax_percentage` decimal(5,2))
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
  'hdfs://HDP50/apps/hive/warehouse/tpcds_bin_partitioned_orc_10.db/web_site'
TBLPROPERTIES (
'hmsMirror_Metadata_Stage1'='2021-05-11 18:41:13',
'hmsMirror_Converted'='true',
'discover.partitions'='true',
'hmsMirror_LegacyManaged'='true',
  'transient_lastDdlTime'='1609880830');
USE tpcds_bin_partitioned_orc_10;
DROP TABLE IF EXISTS tpcds_bin_partitioned_orc_10.transfer_web_site;
CREATE EXTERNAL TABLE `transfer_web_site`(
  `web_site_sk` bigint, 
  `web_site_id` char(16), 
  `web_rec_start_date` date, 
  `web_rec_end_date` date, 
  `web_name` varchar(50), 
  `web_open_date_sk` bigint, 
  `web_close_date_sk` bigint, 
  `web_class` varchar(50), 
  `web_manager` varchar(40), 
  `web_mkt_id` int, 
  `web_mkt_class` varchar(50), 
  `web_mkt_desc` varchar(100), 
  `web_market_manager` varchar(40), 
  `web_company_id` int, 
  `web_company_name` char(50), 
  `web_street_number` char(10), 
  `web_street_name` varchar(60), 
  `web_street_type` char(15), 
  `web_suite_number` char(10), 
  `web_city` varchar(60), 
  `web_county` varchar(30), 
  `web_state` char(2), 
  `web_zip` char(10), 
  `web_country` varchar(20), 
  `web_gmt_offset` decimal(5,2), 
  `web_tax_percentage` decimal(5,2))
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
'hdfs://HOME90/apps/hive/warehouse/tpcds_bin_partitioned_orc_10.db/web_site'
TBLPROPERTIES (
'hmsMirror_Metadata_Stage1'='2021-05-11 18:41:13',
'hmsMirror_LegacyManaged'='true',
'hmsMirror_Converted'='true',
'external.table.purge'='true',
'discover.partitions'='true',
  'transient_lastDdlTime'='1609880830');
USE tpcds_bin_partitioned_orc_10;
FROM web_site INSERT OVERWRITE TABLE transfer_web_site SELECT *;
USE tpcds_bin_partitioned_orc_10;
DROP TABLE IF EXISTS tpcds_bin_partitioned_orc_10.web_site;
 ALTER TABLE transfer_web_site RENAME TO web_site;
