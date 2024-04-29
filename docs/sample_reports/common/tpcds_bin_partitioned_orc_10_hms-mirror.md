# HMS-Mirror for: tpcds_bin_partitioned_orc_10

v.1.2.8.2-SNAPSHOT
---
## Run Log

| Date | Elapsed Time |
|:---|:---|
| 2021-05-11 18:38:40 | 7.40 secs |

## Config:
```
---
dataStrategy: "COMMON"
hybrid:
  sqlPartitionLimit: 100
  sqlSizeLimit: 1073741824
migrateACID: false
execute: false
readOnly: false
sync: false
sqlOutput: true
acceptance:
  silentOverride: null
  backedUpHDFS: null
  backedUpMetastore: null
  trashConfigured: null
  potentialDataLoss: null
tblRegEx: null
dbPrefix: null
databases:
- "tpcds_bin_partitioned_orc_10"
features: null
transfer:
  concurrency: 4
  transferPrefix: "transfer_"
  exportBaseDirPrefix: "/apps/hive/warehouse/export_"
  intermediateStorage: null
clusters:
  LEFT:
    environment: "LEFT"
    legacyHive: true
    hcfsNamespace: "hdfs://HDP50"
    hiveServer2:
      uri: "jdbc:hive2://k02.streever.local:10000/perf_test"
      connectionProperties:
        maxWaitMillis: "5000"
        user: "*****"
        password: "*****"
        maxTotal: "-1"
      jarFile: "/home/dstreev/.hms-mirror/aux_libs/ext/hive-jdbc-1.2.1000.2.6.5.1175-1-standalone.jar"
    partitionDiscovery:
      auto: true
      initMSCK: true
  RIGHT:
    environment: "RIGHT"
    legacyHive: false
    hcfsNamespace: "hdfs://HOME90"
    hiveServer2:
      uri: "jdbc:hive2://s03.streever.local:8443/perf_test;ssl=true;transportMode=http;httpPath=gateway/cdp-proxy-api/hive;sslTrustStore=/home/dstreev/bin/certs/gateway-client-trust.jks;trustStorePassword=changeit"
      connectionProperties:
        maxWaitMillis: "5000"
        user: "*****"
        password: "*****"
        maxTotal: "-1"
      jarFile: "/home/dstreev/.hms-mirror/aux_libs/ext/hive-jdbc-3.1.3000.7.1.6.0-297-standalone.jar"
    partitionDiscovery:
      auto: true
      initMSCK: true

```

## DB Create Statement

```
CREATE DATABASE IF NOT EXISTS tpcds_bin_partitioned_orc_10
LOCATION "hdfs://HDP50/apps/hive/warehouse/tpcds_bin_partitioned_orc_10.db"
```

## DB Issues

none

## Table Status

<table>
<tr>
<th style="test-align:left">Table</th>
<th style="test-align:left">Phase<br/>State</th>
<th style="test-align:right">Duration</th>
<th style="test-align:right">Partition<br/>Count</th>
<th style="test-align:left">Actions</th>
<th style="test-align:left">LEFT Table Actions</th>
<th style="test-align:left">RIGHT Table Actions</th>
<th style="test-align:left">Added<br/>Properties</th>
<th style="test-align:left">Issues</th>
<th style="test-align:left">SQL</th>
</tr>
<tr>
<td>call_center</td>
<td>SUCCESS</td>
<td>.02</td>
<td> </td>
<td>
<table><tr><td style="text-align:left">.00</td><td>init</td><td></td></tr><tr><td style="text-align:left">.24</td><td>LEFT</td><td>Fetched Schema</td></tr><tr><td style="text-align:left">5.24</td><td>TRANSFER</td><td>COMMON</td></tr><tr><td style="text-align:left">.01</td><td>RIGHT Schema Create</td><td>true</td></tr></table></td>
<td>
<table><tr><td style="text-align:left">ALTER TABLE tpcds_bin_partitioned_orc_10.call_center SET TBLPROPERTIES ("EXTERNAL"="true");</td></tr></table></td>
<td>
<table><tr><td style="text-align:left">ALTER TABLE tpcds_bin_partitioned_orc_10.call_center SET TBLPROPERTIES ("external.table.purge"="true");</td></tr></table></td>
<td>
'discover.partitions'='true'<br/>'hmsMirror_Converted'='true'<br/>'hmsMirror_LegacyManaged'='true'<br/>'hmsMirror_Metadata_Stage1'='2021-05-11 18:38:39'<br/></td>
<td>
<ul><li>Schema was Legacy Managed or can be purged. The new schema does NOT have the external.table.purge' flag set.  This must be done manually.</li><li>DRY-RUN mode</li></ul></td>
<td>
USE tpcds_bin_partitioned_orc_10;<br/><br/>CREATE EXTERNAL TABLE `call_center`(<br/>  `cc_call_center_sk` bigint, <br/>  `cc_call_center_id` char(16), <br/>  `cc_rec_start_date` date, <br/>  `cc_rec_end_date` date, <br/>  `cc_closed_date_sk` bigint, <br/>  `cc_open_date_sk` bigint, <br/>  `cc_name` varchar(50), <br/>  `cc_class` varchar(50), <br/>  `cc_employees` int, <br/>  `cc_sq_ft` int, <br/>  `cc_hours` char(20), <br/>  `cc_manager` varchar(40), <br/>  `cc_mkt_id` int, <br/>  `cc_mkt_class` char(50), <br/>  `cc_mkt_desc` varchar(100), <br/>  `cc_market_manager` varchar(40), <br/>  `cc_division` int, <br/>  `cc_division_name` varchar(50), <br/>  `cc_company` int, <br/>  `cc_company_name` char(50), <br/>  `cc_street_number` char(10), <br/>  `cc_street_name` varchar(60), <br/>  `cc_street_type` char(15), <br/>  `cc_suite_number` char(10), <br/>  `cc_city` varchar(60), <br/>  `cc_county` varchar(30), <br/>  `cc_state` char(2), <br/>  `cc_zip` char(10), <br/>  `cc_country` varchar(20), <br/>  `cc_gmt_offset` decimal(5,2), <br/>  `cc_tax_percentage` decimal(5,2))<br/>ROW FORMAT SERDE <br/>  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' <br/>STORED AS INPUTFORMAT <br/>  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' <br/>OUTPUTFORMAT <br/>  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'<br/>LOCATION<br/>'hdfs://HDP50/apps/hive/warehouse/tpcds_bin_partitioned_orc_10.db/call_center'<br/>TBLPROPERTIES (<br/>'hmsMirror_Metadata_Stage1'='2021-05-11 18:38:39',<br/>'hmsMirror_Converted'='true',<br/>'discover.partitions'='true',<br/>'hmsMirror_LegacyManaged'='true',<br/>  'transient_lastDdlTime'='1609880768');<br/><br/></td>
</tr>
<tr>
<td>catalog_page</td>
<td>SUCCESS</td>
<td>.63</td>
<td> </td>
<td>
<table><tr><td style="text-align:left">.00</td><td>init</td><td></td></tr><tr><td style="text-align:left">1.94</td><td>LEFT</td><td>Fetched Schema</td></tr><tr><td style="text-align:left">3.54</td><td>TRANSFER</td><td>COMMON</td></tr><tr><td style="text-align:left">.62</td><td>RIGHT Schema Create</td><td>true</td></tr></table></td>
<td>
<table><tr><td style="text-align:left">ALTER TABLE tpcds_bin_partitioned_orc_10.catalog_page SET TBLPROPERTIES ("EXTERNAL"="true");</td></tr></table></td>
<td>
<table><tr><td style="text-align:left">ALTER TABLE tpcds_bin_partitioned_orc_10.catalog_page SET TBLPROPERTIES ("external.table.purge"="true");</td></tr></table></td>
<td>
'discover.partitions'='true'<br/>'hmsMirror_Converted'='true'<br/>'hmsMirror_LegacyManaged'='true'<br/>'hmsMirror_Metadata_Stage1'='2021-05-11 18:38:39'<br/></td>
<td>
<ul><li>Schema was Legacy Managed or can be purged. The new schema does NOT have the external.table.purge' flag set.  This must be done manually.</li><li>DRY-RUN mode</li></ul></td>
<td>
USE tpcds_bin_partitioned_orc_10;<br/><br/>CREATE EXTERNAL TABLE `catalog_page`(<br/>  `cp_catalog_page_sk` bigint, <br/>  `cp_catalog_page_id` char(16), <br/>  `cp_start_date_sk` bigint, <br/>  `cp_end_date_sk` bigint, <br/>  `cp_department` varchar(50), <br/>  `cp_catalog_number` int, <br/>  `cp_catalog_page_number` int, <br/>  `cp_description` varchar(100), <br/>  `cp_type` varchar(100))<br/>ROW FORMAT SERDE <br/>  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' <br/>STORED AS INPUTFORMAT <br/>  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' <br/>OUTPUTFORMAT <br/>  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'<br/>LOCATION<br/>'hdfs://HDP50/apps/hive/warehouse/tpcds_bin_partitioned_orc_10.db/catalog_page'<br/>TBLPROPERTIES (<br/>'hmsMirror_Metadata_Stage1'='2021-05-11 18:38:39',<br/>'hmsMirror_Converted'='true',<br/>'discover.partitions'='true',<br/>'hmsMirror_LegacyManaged'='true',<br/>  'transient_lastDdlTime'='1609880806');<br/><br/></td>
</tr>
<tr>
<td>catalog_returns</td>
<td>SUCCESS</td>
<td>.88</td>
<td>2092</td>
<td>
<table><tr><td style="text-align:left">.00</td><td>init</td><td></td></tr><tr><td style="text-align:left">2.57</td><td>LEFT</td><td>Fetched Schema</td></tr><tr><td style="text-align:left">2.91</td><td>TRANSFER</td><td>COMMON</td></tr><tr><td style="text-align:left">.37</td><td>RIGHT Schema Create</td><td>true</td></tr><tr><td style="text-align:left">.50</td><td>MSCK ran</td><td>true</td></tr></table></td>
<td>
<table><tr><td style="text-align:left">ALTER TABLE tpcds_bin_partitioned_orc_10.catalog_returns SET TBLPROPERTIES ("EXTERNAL"="true");</td></tr></table></td>
<td>
<table><tr><td style="text-align:left">ALTER TABLE tpcds_bin_partitioned_orc_10.catalog_returns SET TBLPROPERTIES ("external.table.purge"="true");</td></tr></table></td>
<td>
'discover.partitions'='true'<br/>'hmsMirror_Converted'='true'<br/>'hmsMirror_LegacyManaged'='true'<br/>'hmsMirror_Metadata_Stage1'='2021-05-11 18:38:39'<br/></td>
<td>
<ul><li>Schema was Legacy Managed or can be purged. The new schema does NOT have the external.table.purge' flag set.  This must be done manually.</li><li>DRY-RUN mode</li><li>DRY-RUN: MSCK NOT run</li><li>This table has partitions and is set for 'auto' discovery via table property 'discover.partitions'='true'. You've requested an immediate 'MSCK' for the table, so the partitions will be current. For future partition discovery, ensure the Metastore is running the 'PartitionManagementTask' service.</li></ul></td>
<td>
USE tpcds_bin_partitioned_orc_10;<br/><br/>CREATE EXTERNAL TABLE `catalog_returns`(<br/>  `cr_returned_time_sk` bigint, <br/>  `cr_item_sk` bigint, <br/>  `cr_refunded_customer_sk` bigint, <br/>  `cr_refunded_cdemo_sk` bigint, <br/>  `cr_refunded_hdemo_sk` bigint, <br/>  `cr_refunded_addr_sk` bigint, <br/>  `cr_returning_customer_sk` bigint, <br/>  `cr_returning_cdemo_sk` bigint, <br/>  `cr_returning_hdemo_sk` bigint, <br/>  `cr_returning_addr_sk` bigint, <br/>  `cr_call_center_sk` bigint, <br/>  `cr_catalog_page_sk` bigint, <br/>  `cr_ship_mode_sk` bigint, <br/>  `cr_warehouse_sk` bigint, <br/>  `cr_reason_sk` bigint, <br/>  `cr_order_number` bigint, <br/>  `cr_return_quantity` int, <br/>  `cr_return_amount` decimal(7,2), <br/>  `cr_return_tax` decimal(7,2), <br/>  `cr_return_amt_inc_tax` decimal(7,2), <br/>  `cr_fee` decimal(7,2), <br/>  `cr_return_ship_cost` decimal(7,2), <br/>  `cr_refunded_cash` decimal(7,2), <br/>  `cr_reversed_charge` decimal(7,2), <br/>  `cr_store_credit` decimal(7,2), <br/>  `cr_net_loss` decimal(7,2))<br/>PARTITIONED BY ( <br/>  `cr_returned_date_sk` bigint)<br/>ROW FORMAT SERDE <br/>  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' <br/>STORED AS INPUTFORMAT <br/>  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' <br/>OUTPUTFORMAT <br/>  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'<br/>LOCATION<br/>'hdfs://HDP50/apps/hive/warehouse/tpcds_bin_partitioned_orc_10.db/catalog_returns'<br/>TBLPROPERTIES (<br/>'hmsMirror_Metadata_Stage1'='2021-05-11 18:38:39',<br/>'hmsMirror_Converted'='true',<br/>'discover.partitions'='true',<br/>'hmsMirror_LegacyManaged'='true',<br/>  'transient_lastDdlTime'='1609884769');<br/><br/>MSCK REPAIR TABLE tpcds_bin_partitioned_orc_10.catalog_returns;<br/><br/></td>
</tr>
<tr>
<td>catalog_sales</td>
<td>SUCCESS</td>
<td>.88</td>
<td>1836</td>
<td>
<table><tr><td style="text-align:left">.00</td><td>init</td><td></td></tr><tr><td style="text-align:left">2.54</td><td>LEFT</td><td>Fetched Schema</td></tr><tr><td style="text-align:left">2.95</td><td>TRANSFER</td><td>COMMON</td></tr><tr><td style="text-align:left">.62</td><td>RIGHT Schema Create</td><td>true</td></tr><tr><td style="text-align:left">.25</td><td>MSCK ran</td><td>true</td></tr></table></td>
<td>
<table><tr><td style="text-align:left">ALTER TABLE tpcds_bin_partitioned_orc_10.catalog_sales SET TBLPROPERTIES ("EXTERNAL"="true");</td></tr></table></td>
<td>
<table><tr><td style="text-align:left">ALTER TABLE tpcds_bin_partitioned_orc_10.catalog_sales SET TBLPROPERTIES ("external.table.purge"="true");</td></tr></table></td>
<td>
'discover.partitions'='true'<br/>'hmsMirror_Converted'='true'<br/>'hmsMirror_LegacyManaged'='true'<br/>'hmsMirror_Metadata_Stage1'='2021-05-11 18:38:39'<br/></td>
<td>
<ul><li>Schema was Legacy Managed or can be purged. The new schema does NOT have the external.table.purge' flag set.  This must be done manually.</li><li>DRY-RUN mode</li><li>DRY-RUN: MSCK NOT run</li><li>This table has partitions and is set for 'auto' discovery via table property 'discover.partitions'='true'. You've requested an immediate 'MSCK' for the table, so the partitions will be current. For future partition discovery, ensure the Metastore is running the 'PartitionManagementTask' service.</li></ul></td>
<td>
USE tpcds_bin_partitioned_orc_10;<br/><br/>CREATE EXTERNAL TABLE `catalog_sales`(<br/>  `cs_sold_time_sk` bigint, <br/>  `cs_ship_date_sk` bigint, <br/>  `cs_bill_customer_sk` bigint, <br/>  `cs_bill_cdemo_sk` bigint, <br/>  `cs_bill_hdemo_sk` bigint, <br/>  `cs_bill_addr_sk` bigint, <br/>  `cs_ship_customer_sk` bigint, <br/>  `cs_ship_cdemo_sk` bigint, <br/>  `cs_ship_hdemo_sk` bigint, <br/>  `cs_ship_addr_sk` bigint, <br/>  `cs_call_center_sk` bigint, <br/>  `cs_catalog_page_sk` bigint, <br/>  `cs_ship_mode_sk` bigint, <br/>  `cs_warehouse_sk` bigint, <br/>  `cs_item_sk` bigint, <br/>  `cs_promo_sk` bigint, <br/>  `cs_order_number` bigint, <br/>  `cs_quantity` int, <br/>  `cs_wholesale_cost` decimal(7,2), <br/>  `cs_list_price` decimal(7,2), <br/>  `cs_sales_price` decimal(7,2), <br/>  `cs_ext_discount_amt` decimal(7,2), <br/>  `cs_ext_sales_price` decimal(7,2), <br/>  `cs_ext_wholesale_cost` decimal(7,2), <br/>  `cs_ext_list_price` decimal(7,2), <br/>  `cs_ext_tax` decimal(7,2), <br/>  `cs_coupon_amt` decimal(7,2), <br/>  `cs_ext_ship_cost` decimal(7,2), <br/>  `cs_net_paid` decimal(7,2), <br/>  `cs_net_paid_inc_tax` decimal(7,2), <br/>  `cs_net_paid_inc_ship` decimal(7,2), <br/>  `cs_net_paid_inc_ship_tax` decimal(7,2), <br/>  `cs_net_profit` decimal(7,2))<br/>PARTITIONED BY ( <br/>  `cs_sold_date_sk` bigint)<br/>ROW FORMAT SERDE <br/>  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' <br/>STORED AS INPUTFORMAT <br/>  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' <br/>OUTPUTFORMAT <br/>  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'<br/>LOCATION<br/>'hdfs://HDP50/apps/hive/warehouse/tpcds_bin_partitioned_orc_10.db/catalog_sales'<br/>TBLPROPERTIES (<br/>'hmsMirror_Metadata_Stage1'='2021-05-11 18:38:39',<br/>'hmsMirror_Converted'='true',<br/>'discover.partitions'='true',<br/>'hmsMirror_LegacyManaged'='true',<br/>  'transient_lastDdlTime'='1609884273');<br/><br/>MSCK REPAIR TABLE tpcds_bin_partitioned_orc_10.catalog_sales;<br/><br/></td>
</tr>
<tr>
<td>customer</td>
<td>SUCCESS</td>
<td>.37</td>
<td> </td>
<td>
<table><tr><td style="text-align:left">.00</td><td>init</td><td></td></tr><tr><td style="text-align:left">2.33</td><td>LEFT</td><td>Fetched Schema</td></tr><tr><td style="text-align:left">3.16</td><td>TRANSFER</td><td>COMMON</td></tr><tr><td style="text-align:left">.37</td><td>RIGHT Schema Create</td><td>true</td></tr></table></td>
<td>
<table><tr><td style="text-align:left">ALTER TABLE tpcds_bin_partitioned_orc_10.customer SET TBLPROPERTIES ("EXTERNAL"="true");</td></tr></table></td>
<td>
<table><tr><td style="text-align:left">ALTER TABLE tpcds_bin_partitioned_orc_10.customer SET TBLPROPERTIES ("external.table.purge"="true");</td></tr></table></td>
<td>
'discover.partitions'='true'<br/>'hmsMirror_Converted'='true'<br/>'hmsMirror_LegacyManaged'='true'<br/>'hmsMirror_Metadata_Stage1'='2021-05-11 18:38:39'<br/></td>
<td>
<ul><li>Schema was Legacy Managed or can be purged. The new schema does NOT have the external.table.purge' flag set.  This must be done manually.</li><li>DRY-RUN mode</li></ul></td>
<td>
USE tpcds_bin_partitioned_orc_10;<br/><br/>CREATE EXTERNAL TABLE `customer`(<br/>  `c_customer_sk` bigint, <br/>  `c_customer_id` char(16), <br/>  `c_current_cdemo_sk` bigint, <br/>  `c_current_hdemo_sk` bigint, <br/>  `c_current_addr_sk` bigint, <br/>  `c_first_shipto_date_sk` bigint, <br/>  `c_first_sales_date_sk` bigint, <br/>  `c_salutation` char(10), <br/>  `c_first_name` char(20), <br/>  `c_last_name` char(30), <br/>  `c_preferred_cust_flag` char(1), <br/>  `c_birth_day` int, <br/>  `c_birth_month` int, <br/>  `c_birth_year` int, <br/>  `c_birth_country` varchar(20), <br/>  `c_login` char(13), <br/>  `c_email_address` char(50), <br/>  `c_last_review_date_sk` bigint)<br/>ROW FORMAT SERDE <br/>  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' <br/>STORED AS INPUTFORMAT <br/>  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' <br/>OUTPUTFORMAT <br/>  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'<br/>LOCATION<br/>'hdfs://HDP50/apps/hive/warehouse/tpcds_bin_partitioned_orc_10.db/customer'<br/>TBLPROPERTIES (<br/>'hmsMirror_Metadata_Stage1'='2021-05-11 18:38:39',<br/>'hmsMirror_Converted'='true',<br/>'discover.partitions'='true',<br/>'hmsMirror_LegacyManaged'='true',<br/>  'transient_lastDdlTime'='1609880557');<br/><br/></td>
</tr>
<tr>
<td>customer_address</td>
<td>SUCCESS</td>
<td>.24</td>
<td> </td>
<td>
<table><tr><td style="text-align:left">.00</td><td>init</td><td></td></tr><tr><td style="text-align:left">2.67</td><td>LEFT</td><td>Fetched Schema</td></tr><tr><td style="text-align:left">3.19</td><td>TRANSFER</td><td>COMMON</td></tr><tr><td style="text-align:left">.24</td><td>RIGHT Schema Create</td><td>true</td></tr></table></td>
<td>
<table><tr><td style="text-align:left">ALTER TABLE tpcds_bin_partitioned_orc_10.customer_address SET TBLPROPERTIES ("EXTERNAL"="true");</td></tr></table></td>
<td>
<table><tr><td style="text-align:left">ALTER TABLE tpcds_bin_partitioned_orc_10.customer_address SET TBLPROPERTIES ("external.table.purge"="true");</td></tr></table></td>
<td>
'discover.partitions'='true'<br/>'hmsMirror_Converted'='true'<br/>'hmsMirror_LegacyManaged'='true'<br/>'hmsMirror_Metadata_Stage1'='2021-05-11 18:38:39'<br/></td>
<td>
<ul><li>Schema was Legacy Managed or can be purged. The new schema does NOT have the external.table.purge' flag set.  This must be done manually.</li><li>DRY-RUN mode</li></ul></td>
<td>
USE tpcds_bin_partitioned_orc_10;<br/><br/>CREATE EXTERNAL TABLE `customer_address`(<br/>  `ca_address_sk` bigint, <br/>  `ca_address_id` char(16), <br/>  `ca_street_number` char(10), <br/>  `ca_street_name` varchar(60), <br/>  `ca_street_type` char(15), <br/>  `ca_suite_number` char(10), <br/>  `ca_city` varchar(60), <br/>  `ca_county` varchar(30), <br/>  `ca_state` char(2), <br/>  `ca_zip` char(10), <br/>  `ca_country` varchar(20), <br/>  `ca_gmt_offset` decimal(5,2), <br/>  `ca_location_type` char(20))<br/>ROW FORMAT SERDE <br/>  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' <br/>STORED AS INPUTFORMAT <br/>  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' <br/>OUTPUTFORMAT <br/>  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'<br/>LOCATION<br/>'hdfs://HDP50/apps/hive/warehouse/tpcds_bin_partitioned_orc_10.db/customer_address'<br/>TBLPROPERTIES (<br/>'hmsMirror_Metadata_Stage1'='2021-05-11 18:38:39',<br/>'hmsMirror_Converted'='true',<br/>'discover.partitions'='true',<br/>'hmsMirror_LegacyManaged'='true',<br/>  'transient_lastDdlTime'='1609880632');<br/><br/></td>
</tr>
<tr>
<td>customer_demographics</td>
<td>SUCCESS</td>
<td>.26</td>
<td> </td>
<td>
<table><tr><td style="text-align:left">.00</td><td>init</td><td></td></tr><tr><td style="text-align:left">2.86</td><td>LEFT</td><td>Fetched Schema</td></tr><tr><td style="text-align:left">3.24</td><td>TRANSFER</td><td>COMMON</td></tr><tr><td style="text-align:left">.26</td><td>RIGHT Schema Create</td><td>true</td></tr></table></td>
<td>
<table><tr><td style="text-align:left">ALTER TABLE tpcds_bin_partitioned_orc_10.customer_demographics SET TBLPROPERTIES ("EXTERNAL"="true");</td></tr></table></td>
<td>
<table><tr><td style="text-align:left">ALTER TABLE tpcds_bin_partitioned_orc_10.customer_demographics SET TBLPROPERTIES ("external.table.purge"="true");</td></tr></table></td>
<td>
'discover.partitions'='true'<br/>'hmsMirror_Converted'='true'<br/>'hmsMirror_LegacyManaged'='true'<br/>'hmsMirror_Metadata_Stage1'='2021-05-11 18:38:39'<br/></td>
<td>
<ul><li>Schema was Legacy Managed or can be purged. The new schema does NOT have the external.table.purge' flag set.  This must be done manually.</li><li>DRY-RUN mode</li></ul></td>
<td>
USE tpcds_bin_partitioned_orc_10;<br/><br/>CREATE EXTERNAL TABLE `customer_demographics`(<br/>  `cd_demo_sk` bigint, <br/>  `cd_gender` char(1), <br/>  `cd_marital_status` char(1), <br/>  `cd_education_status` char(20), <br/>  `cd_purchase_estimate` int, <br/>  `cd_credit_rating` char(10), <br/>  `cd_dep_count` int, <br/>  `cd_dep_employed_count` int, <br/>  `cd_dep_college_count` int)<br/>ROW FORMAT SERDE <br/>  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' <br/>STORED AS INPUTFORMAT <br/>  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' <br/>OUTPUTFORMAT <br/>  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'<br/>LOCATION<br/>'hdfs://HDP50/apps/hive/warehouse/tpcds_bin_partitioned_orc_10.db/customer_demographics'<br/>TBLPROPERTIES (<br/>'hmsMirror_Metadata_Stage1'='2021-05-11 18:38:39',<br/>'hmsMirror_Converted'='true',<br/>'discover.partitions'='true',<br/>'hmsMirror_LegacyManaged'='true',<br/>  'transient_lastDdlTime'='1609880581');<br/><br/></td>
</tr>
<tr>
<td>date_dim</td>
<td>SUCCESS</td>
<td>.26</td>
<td> </td>
<td>
<table><tr><td style="text-align:left">.00</td><td>init</td><td></td></tr><tr><td style="text-align:left">3.24</td><td>LEFT</td><td>Fetched Schema</td></tr><tr><td style="text-align:left">2.86</td><td>TRANSFER</td><td>COMMON</td></tr><tr><td style="text-align:left">.26</td><td>RIGHT Schema Create</td><td>true</td></tr></table></td>
<td>
<table><tr><td style="text-align:left">ALTER TABLE tpcds_bin_partitioned_orc_10.date_dim SET TBLPROPERTIES ("EXTERNAL"="true");</td></tr></table></td>
<td>
<table><tr><td style="text-align:left">ALTER TABLE tpcds_bin_partitioned_orc_10.date_dim SET TBLPROPERTIES ("external.table.purge"="true");</td></tr></table></td>
<td>
'discover.partitions'='true'<br/>'hmsMirror_Converted'='true'<br/>'hmsMirror_LegacyManaged'='true'<br/>'hmsMirror_Metadata_Stage1'='2021-05-11 18:38:39'<br/></td>
<td>
<ul><li>Schema was Legacy Managed or can be purged. The new schema does NOT have the external.table.purge' flag set.  This must be done manually.</li><li>DRY-RUN mode</li></ul></td>
<td>
USE tpcds_bin_partitioned_orc_10;<br/><br/>CREATE EXTERNAL TABLE `date_dim`(<br/>  `d_date_sk` bigint, <br/>  `d_date_id` char(16), <br/>  `d_date` date, <br/>  `d_month_seq` int, <br/>  `d_week_seq` int, <br/>  `d_quarter_seq` int, <br/>  `d_year` int, <br/>  `d_dow` int, <br/>  `d_moy` int, <br/>  `d_dom` int, <br/>  `d_qoy` int, <br/>  `d_fy_year` int, <br/>  `d_fy_quarter_seq` int, <br/>  `d_fy_week_seq` int, <br/>  `d_day_name` char(9), <br/>  `d_quarter_name` char(6), <br/>  `d_holiday` char(1), <br/>  `d_weekend` char(1), <br/>  `d_following_holiday` char(1), <br/>  `d_first_dom` int, <br/>  `d_last_dom` int, <br/>  `d_same_day_ly` int, <br/>  `d_same_day_lq` int, <br/>  `d_current_day` char(1), <br/>  `d_current_week` char(1), <br/>  `d_current_month` char(1), <br/>  `d_current_quarter` char(1), <br/>  `d_current_year` char(1))<br/>ROW FORMAT SERDE <br/>  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' <br/>STORED AS INPUTFORMAT <br/>  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' <br/>OUTPUTFORMAT <br/>  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'<br/>LOCATION<br/>'hdfs://HDP50/apps/hive/warehouse/tpcds_bin_partitioned_orc_10.db/date_dim'<br/>TBLPROPERTIES (<br/>'hmsMirror_Metadata_Stage1'='2021-05-11 18:38:39',<br/>'hmsMirror_Converted'='true',<br/>'discover.partitions'='true',<br/>'hmsMirror_LegacyManaged'='true',<br/>  'transient_lastDdlTime'='1609880485');<br/><br/></td>
</tr>
<tr>
<td>household_demographics</td>
<td>SUCCESS</td>
<td>.04</td>
<td> </td>
<td>
<table><tr><td style="text-align:left">.00</td><td>init</td><td></td></tr><tr><td style="text-align:left">3.47</td><td>LEFT</td><td>Fetched Schema</td></tr><tr><td style="text-align:left">2.89</td><td>TRANSFER</td><td>COMMON</td></tr><tr><td style="text-align:left">.03</td><td>RIGHT Schema Create</td><td>true</td></tr></table></td>
<td>
<table><tr><td style="text-align:left">ALTER TABLE tpcds_bin_partitioned_orc_10.household_demographics SET TBLPROPERTIES ("EXTERNAL"="true");</td></tr></table></td>
<td>
<table><tr><td style="text-align:left">ALTER TABLE tpcds_bin_partitioned_orc_10.household_demographics SET TBLPROPERTIES ("external.table.purge"="true");</td></tr></table></td>
<td>
'discover.partitions'='true'<br/>'hmsMirror_Converted'='true'<br/>'hmsMirror_LegacyManaged'='true'<br/>'hmsMirror_Metadata_Stage1'='2021-05-11 18:38:39'<br/></td>
<td>
<ul><li>Schema was Legacy Managed or can be purged. The new schema does NOT have the external.table.purge' flag set.  This must be done manually.</li><li>DRY-RUN mode</li></ul></td>
<td>
USE tpcds_bin_partitioned_orc_10;<br/><br/>CREATE EXTERNAL TABLE `household_demographics`(<br/>  `hd_demo_sk` bigint, <br/>  `hd_income_band_sk` bigint, <br/>  `hd_buy_potential` char(15), <br/>  `hd_dep_count` int, <br/>  `hd_vehicle_count` int)<br/>ROW FORMAT SERDE <br/>  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' <br/>STORED AS INPUTFORMAT <br/>  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' <br/>OUTPUTFORMAT <br/>  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'<br/>LOCATION<br/>'hdfs://HDP50/apps/hive/warehouse/tpcds_bin_partitioned_orc_10.db/household_demographics'<br/>TBLPROPERTIES (<br/>'hmsMirror_Metadata_Stage1'='2021-05-11 18:38:39',<br/>'hmsMirror_Converted'='true',<br/>'discover.partitions'='true',<br/>'hmsMirror_LegacyManaged'='true',<br/>  'transient_lastDdlTime'='1609880604');<br/><br/></td>
</tr>
<tr>
<td>income_band</td>
<td>SUCCESS</td>
<td>.01</td>
<td> </td>
<td>
<table><tr><td style="text-align:left">.00</td><td>init</td><td></td></tr><tr><td style="text-align:left">3.56</td><td>LEFT</td><td>Fetched Schema</td></tr><tr><td style="text-align:left">2.80</td><td>TRANSFER</td><td>COMMON</td></tr><tr><td style="text-align:left">.00</td><td>RIGHT Schema Create</td><td>true</td></tr></table></td>
<td>
<table><tr><td style="text-align:left">ALTER TABLE tpcds_bin_partitioned_orc_10.income_band SET TBLPROPERTIES ("EXTERNAL"="true");</td></tr></table></td>
<td>
<table><tr><td style="text-align:left">ALTER TABLE tpcds_bin_partitioned_orc_10.income_band SET TBLPROPERTIES ("external.table.purge"="true");</td></tr></table></td>
<td>
'discover.partitions'='true'<br/>'hmsMirror_Converted'='true'<br/>'hmsMirror_LegacyManaged'='true'<br/>'hmsMirror_Metadata_Stage1'='2021-05-11 18:38:39'<br/></td>
<td>
<ul><li>Schema was Legacy Managed or can be purged. The new schema does NOT have the external.table.purge' flag set.  This must be done manually.</li><li>DRY-RUN mode</li></ul></td>
<td>
USE tpcds_bin_partitioned_orc_10;<br/><br/>CREATE EXTERNAL TABLE `income_band`(<br/>  `ib_income_band_sk` bigint, <br/>  `ib_lower_bound` int, <br/>  `ib_upper_bound` int)<br/>ROW FORMAT SERDE <br/>  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' <br/>STORED AS INPUTFORMAT <br/>  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' <br/>OUTPUTFORMAT <br/>  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'<br/>LOCATION<br/>'hdfs://HDP50/apps/hive/warehouse/tpcds_bin_partitioned_orc_10.db/income_band'<br/>TBLPROPERTIES (<br/>'hmsMirror_Metadata_Stage1'='2021-05-11 18:38:39',<br/>'hmsMirror_Converted'='true',<br/>'discover.partitions'='true',<br/>'hmsMirror_LegacyManaged'='true',<br/>  'transient_lastDdlTime'='1609880750');<br/><br/></td>
</tr>
<tr>
<td>inventory</td>
<td>SUCCESS</td>
<td>.02</td>
<td> </td>
<td>
<table><tr><td style="text-align:left">.00</td><td>init</td><td></td></tr><tr><td style="text-align:left">3.65</td><td>LEFT</td><td>Fetched Schema</td></tr><tr><td style="text-align:left">2.71</td><td>TRANSFER</td><td>COMMON</td></tr><tr><td style="text-align:left">.02</td><td>RIGHT Schema Create</td><td>true</td></tr></table></td>
<td>
<table><tr><td style="text-align:left">ALTER TABLE tpcds_bin_partitioned_orc_10.inventory SET TBLPROPERTIES ("EXTERNAL"="true");</td></tr></table></td>
<td>
<table><tr><td style="text-align:left">ALTER TABLE tpcds_bin_partitioned_orc_10.inventory SET TBLPROPERTIES ("external.table.purge"="true");</td></tr></table></td>
<td>
'discover.partitions'='true'<br/>'hmsMirror_Converted'='true'<br/>'hmsMirror_LegacyManaged'='true'<br/>'hmsMirror_Metadata_Stage1'='2021-05-11 18:38:39'<br/></td>
<td>
<ul><li>Schema was Legacy Managed or can be purged. The new schema does NOT have the external.table.purge' flag set.  This must be done manually.</li><li>DRY-RUN mode</li></ul></td>
<td>
USE tpcds_bin_partitioned_orc_10;<br/><br/>CREATE EXTERNAL TABLE `inventory`(<br/>  `inv_date_sk` bigint, <br/>  `inv_item_sk` bigint, <br/>  `inv_warehouse_sk` bigint, <br/>  `inv_quantity_on_hand` int)<br/>ROW FORMAT SERDE <br/>  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' <br/>STORED AS INPUTFORMAT <br/>  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' <br/>OUTPUTFORMAT <br/>  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'<br/>LOCATION<br/>'hdfs://HDP50/apps/hive/warehouse/tpcds_bin_partitioned_orc_10.db/inventory'<br/>TBLPROPERTIES (<br/>'hmsMirror_Metadata_Stage1'='2021-05-11 18:38:39',<br/>'hmsMirror_Converted'='true',<br/>'discover.partitions'='true',<br/>'hmsMirror_LegacyManaged'='true',<br/>  'transient_lastDdlTime'='1609886025');<br/><br/></td>
</tr>
<tr>
<td>item</td>
<td>SUCCESS</td>
<td>.03</td>
<td> </td>
<td>
<table><tr><td style="text-align:left">.00</td><td>init</td><td></td></tr><tr><td style="text-align:left">3.75</td><td>LEFT</td><td>Fetched Schema</td></tr><tr><td style="text-align:left">2.61</td><td>TRANSFER</td><td>COMMON</td></tr><tr><td style="text-align:left">.02</td><td>RIGHT Schema Create</td><td>true</td></tr></table></td>
<td>
<table><tr><td style="text-align:left">ALTER TABLE tpcds_bin_partitioned_orc_10.item SET TBLPROPERTIES ("EXTERNAL"="true");</td></tr></table></td>
<td>
<table><tr><td style="text-align:left">ALTER TABLE tpcds_bin_partitioned_orc_10.item SET TBLPROPERTIES ("external.table.purge"="true");</td></tr></table></td>
<td>
'discover.partitions'='true'<br/>'hmsMirror_Converted'='true'<br/>'hmsMirror_LegacyManaged'='true'<br/>'hmsMirror_Metadata_Stage1'='2021-05-11 18:38:39'<br/></td>
<td>
<ul><li>Schema was Legacy Managed or can be purged. The new schema does NOT have the external.table.purge' flag set.  This must be done manually.</li><li>DRY-RUN mode</li></ul></td>
<td>
USE tpcds_bin_partitioned_orc_10;<br/><br/>CREATE EXTERNAL TABLE `item`(<br/>  `i_item_sk` bigint, <br/>  `i_item_id` char(16), <br/>  `i_rec_start_date` date, <br/>  `i_rec_end_date` date, <br/>  `i_item_desc` varchar(200), <br/>  `i_current_price` decimal(7,2), <br/>  `i_wholesale_cost` decimal(7,2), <br/>  `i_brand_id` int, <br/>  `i_brand` char(50), <br/>  `i_class_id` int, <br/>  `i_class` char(50), <br/>  `i_category_id` int, <br/>  `i_category` char(50), <br/>  `i_manufact_id` int, <br/>  `i_manufact` char(50), <br/>  `i_size` char(20), <br/>  `i_formulation` char(20), <br/>  `i_color` char(20), <br/>  `i_units` char(10), <br/>  `i_container` char(10), <br/>  `i_manager_id` int, <br/>  `i_product_name` char(50))<br/>ROW FORMAT SERDE <br/>  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' <br/>STORED AS INPUTFORMAT <br/>  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' <br/>OUTPUTFORMAT <br/>  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'<br/>LOCATION<br/>'hdfs://HDP50/apps/hive/warehouse/tpcds_bin_partitioned_orc_10.db/item'<br/>TBLPROPERTIES (<br/>'hmsMirror_Metadata_Stage1'='2021-05-11 18:38:39',<br/>'hmsMirror_Converted'='true',<br/>'discover.partitions'='true',<br/>'hmsMirror_LegacyManaged'='true',<br/>  'transient_lastDdlTime'='1609880532');<br/><br/></td>
</tr>
<tr>
<td>promotion</td>
<td>SUCCESS</td>
<td>.01</td>
<td> </td>
<td>
<table><tr><td style="text-align:left">.00</td><td>init</td><td></td></tr><tr><td style="text-align:left">3.85</td><td>LEFT</td><td>Fetched Schema</td></tr><tr><td style="text-align:left">2.52</td><td>TRANSFER</td><td>COMMON</td></tr><tr><td style="text-align:left">.00</td><td>RIGHT Schema Create</td><td>true</td></tr></table></td>
<td>
<table><tr><td style="text-align:left">ALTER TABLE tpcds_bin_partitioned_orc_10.promotion SET TBLPROPERTIES ("EXTERNAL"="true");</td></tr></table></td>
<td>
<table><tr><td style="text-align:left">ALTER TABLE tpcds_bin_partitioned_orc_10.promotion SET TBLPROPERTIES ("external.table.purge"="true");</td></tr></table></td>
<td>
'discover.partitions'='true'<br/>'hmsMirror_Converted'='true'<br/>'hmsMirror_LegacyManaged'='true'<br/>'hmsMirror_Metadata_Stage1'='2021-05-11 18:38:39'<br/></td>
<td>
<ul><li>Schema was Legacy Managed or can be purged. The new schema does NOT have the external.table.purge' flag set.  This must be done manually.</li><li>DRY-RUN mode</li></ul></td>
<td>
USE tpcds_bin_partitioned_orc_10;<br/><br/>CREATE EXTERNAL TABLE `promotion`(<br/>  `p_promo_sk` bigint, <br/>  `p_promo_id` char(16), <br/>  `p_start_date_sk` bigint, <br/>  `p_end_date_sk` bigint, <br/>  `p_item_sk` bigint, <br/>  `p_cost` decimal(15,2), <br/>  `p_response_target` int, <br/>  `p_promo_name` char(50), <br/>  `p_channel_dmail` char(1), <br/>  `p_channel_email` char(1), <br/>  `p_channel_catalog` char(1), <br/>  `p_channel_tv` char(1), <br/>  `p_channel_radio` char(1), <br/>  `p_channel_press` char(1), <br/>  `p_channel_event` char(1), <br/>  `p_channel_demo` char(1), <br/>  `p_channel_details` varchar(100), <br/>  `p_purpose` char(15), <br/>  `p_discount_active` char(1))<br/>ROW FORMAT SERDE <br/>  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' <br/>STORED AS INPUTFORMAT <br/>  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' <br/>OUTPUTFORMAT <br/>  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'<br/>LOCATION<br/>'hdfs://HDP50/apps/hive/warehouse/tpcds_bin_partitioned_orc_10.db/promotion'<br/>TBLPROPERTIES (<br/>'hmsMirror_Metadata_Stage1'='2021-05-11 18:38:39',<br/>'hmsMirror_Converted'='true',<br/>'discover.partitions'='true',<br/>'hmsMirror_LegacyManaged'='true',<br/>  'transient_lastDdlTime'='1609880670');<br/><br/></td>
</tr>
<tr>
<td>reason</td>
<td>SUCCESS</td>
<td>.01</td>
<td> </td>
<td>
<table><tr><td style="text-align:left">.00</td><td>init</td><td></td></tr><tr><td style="text-align:left">3.94</td><td>LEFT</td><td>Fetched Schema</td></tr><tr><td style="text-align:left">2.43</td><td>TRANSFER</td><td>COMMON</td></tr><tr><td style="text-align:left">.01</td><td>RIGHT Schema Create</td><td>true</td></tr></table></td>
<td>
<table><tr><td style="text-align:left">ALTER TABLE tpcds_bin_partitioned_orc_10.reason SET TBLPROPERTIES ("EXTERNAL"="true");</td></tr></table></td>
<td>
<table><tr><td style="text-align:left">ALTER TABLE tpcds_bin_partitioned_orc_10.reason SET TBLPROPERTIES ("external.table.purge"="true");</td></tr></table></td>
<td>
'discover.partitions'='true'<br/>'hmsMirror_Converted'='true'<br/>'hmsMirror_LegacyManaged'='true'<br/>'hmsMirror_Metadata_Stage1'='2021-05-11 18:38:39'<br/></td>
<td>
<ul><li>Schema was Legacy Managed or can be purged. The new schema does NOT have the external.table.purge' flag set.  This must be done manually.</li><li>DRY-RUN mode</li></ul></td>
<td>
USE tpcds_bin_partitioned_orc_10;<br/><br/>CREATE EXTERNAL TABLE `reason`(<br/>  `r_reason_sk` bigint, <br/>  `r_reason_id` char(16), <br/>  `r_reason_desc` char(100))<br/>ROW FORMAT SERDE <br/>  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' <br/>STORED AS INPUTFORMAT <br/>  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' <br/>OUTPUTFORMAT <br/>  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'<br/>LOCATION<br/>'hdfs://HDP50/apps/hive/warehouse/tpcds_bin_partitioned_orc_10.db/reason'<br/>TBLPROPERTIES (<br/>'hmsMirror_Metadata_Stage1'='2021-05-11 18:38:39',<br/>'hmsMirror_Converted'='true',<br/>'discover.partitions'='true',<br/>'hmsMirror_LegacyManaged'='true',<br/>  'transient_lastDdlTime'='1609880728');<br/><br/></td>
</tr>
<tr>
<td>ship_mode</td>
<td>SUCCESS</td>
<td>.00</td>
<td> </td>
<td>
<table><tr><td style="text-align:left">.00</td><td>init</td><td></td></tr><tr><td style="text-align:left">4.03</td><td>LEFT</td><td>Fetched Schema</td></tr><tr><td style="text-align:left">2.34</td><td>TRANSFER</td><td>COMMON</td></tr><tr><td style="text-align:left">.00</td><td>RIGHT Schema Create</td><td>true</td></tr></table></td>
<td>
<table><tr><td style="text-align:left">ALTER TABLE tpcds_bin_partitioned_orc_10.ship_mode SET TBLPROPERTIES ("EXTERNAL"="true");</td></tr></table></td>
<td>
<table><tr><td style="text-align:left">ALTER TABLE tpcds_bin_partitioned_orc_10.ship_mode SET TBLPROPERTIES ("external.table.purge"="true");</td></tr></table></td>
<td>
'discover.partitions'='true'<br/>'hmsMirror_Converted'='true'<br/>'hmsMirror_LegacyManaged'='true'<br/>'hmsMirror_Metadata_Stage1'='2021-05-11 18:38:39'<br/></td>
<td>
<ul><li>Schema was Legacy Managed or can be purged. The new schema does NOT have the external.table.purge' flag set.  This must be done manually.</li><li>DRY-RUN mode</li></ul></td>
<td>
USE tpcds_bin_partitioned_orc_10;<br/><br/>CREATE EXTERNAL TABLE `ship_mode`(<br/>  `sm_ship_mode_sk` bigint, <br/>  `sm_ship_mode_id` char(16), <br/>  `sm_type` char(30), <br/>  `sm_code` char(10), <br/>  `sm_carrier` char(20), <br/>  `sm_contract` char(20))<br/>ROW FORMAT SERDE <br/>  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' <br/>STORED AS INPUTFORMAT <br/>  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' <br/>OUTPUTFORMAT <br/>  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'<br/>LOCATION<br/>'hdfs://HDP50/apps/hive/warehouse/tpcds_bin_partitioned_orc_10.db/ship_mode'<br/>TBLPROPERTIES (<br/>'hmsMirror_Metadata_Stage1'='2021-05-11 18:38:39',<br/>'hmsMirror_Converted'='true',<br/>'discover.partitions'='true',<br/>'hmsMirror_LegacyManaged'='true',<br/>  'transient_lastDdlTime'='1609880708');<br/><br/></td>
</tr>
<tr>
<td>store</td>
<td>SUCCESS</td>
<td>.01</td>
<td> </td>
<td>
<table><tr><td style="text-align:left">.00</td><td>init</td><td></td></tr><tr><td style="text-align:left">4.12</td><td>LEFT</td><td>Fetched Schema</td></tr><tr><td style="text-align:left">2.26</td><td>TRANSFER</td><td>COMMON</td></tr><tr><td style="text-align:left">.01</td><td>RIGHT Schema Create</td><td>true</td></tr></table></td>
<td>
<table><tr><td style="text-align:left">ALTER TABLE tpcds_bin_partitioned_orc_10.store SET TBLPROPERTIES ("EXTERNAL"="true");</td></tr></table></td>
<td>
<table><tr><td style="text-align:left">ALTER TABLE tpcds_bin_partitioned_orc_10.store SET TBLPROPERTIES ("external.table.purge"="true");</td></tr></table></td>
<td>
'discover.partitions'='true'<br/>'hmsMirror_Converted'='true'<br/>'hmsMirror_LegacyManaged'='true'<br/>'hmsMirror_Metadata_Stage1'='2021-05-11 18:38:39'<br/></td>
<td>
<ul><li>Schema was Legacy Managed or can be purged. The new schema does NOT have the external.table.purge' flag set.  This must be done manually.</li><li>DRY-RUN mode</li></ul></td>
<td>
USE tpcds_bin_partitioned_orc_10;<br/><br/>CREATE EXTERNAL TABLE `store`(<br/>  `s_store_sk` bigint, <br/>  `s_store_id` char(16), <br/>  `s_rec_start_date` date, <br/>  `s_rec_end_date` date, <br/>  `s_closed_date_sk` bigint, <br/>  `s_store_name` varchar(50), <br/>  `s_number_employees` int, <br/>  `s_floor_space` int, <br/>  `s_hours` char(20), <br/>  `s_manager` varchar(40), <br/>  `s_market_id` int, <br/>  `s_geography_class` varchar(100), <br/>  `s_market_desc` varchar(100), <br/>  `s_market_manager` varchar(40), <br/>  `s_division_id` int, <br/>  `s_division_name` varchar(50), <br/>  `s_company_id` int, <br/>  `s_company_name` varchar(50), <br/>  `s_street_number` varchar(10), <br/>  `s_street_name` varchar(60), <br/>  `s_street_type` char(15), <br/>  `s_suite_number` char(10), <br/>  `s_city` varchar(60), <br/>  `s_county` varchar(30), <br/>  `s_state` char(2), <br/>  `s_zip` char(10), <br/>  `s_country` varchar(20), <br/>  `s_gmt_offset` decimal(5,2), <br/>  `s_tax_percentage` decimal(5,2))<br/>ROW FORMAT SERDE <br/>  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' <br/>STORED AS INPUTFORMAT <br/>  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' <br/>OUTPUTFORMAT <br/>  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'<br/>LOCATION<br/>'hdfs://HDP50/apps/hive/warehouse/tpcds_bin_partitioned_orc_10.db/store'<br/>TBLPROPERTIES (<br/>'hmsMirror_Metadata_Stage1'='2021-05-11 18:38:39',<br/>'hmsMirror_Converted'='true',<br/>'discover.partitions'='true',<br/>'hmsMirror_LegacyManaged'='true',<br/>  'transient_lastDdlTime'='1609880651');<br/><br/></td>
</tr>
<tr>
<td>store_returns</td>
<td>SUCCESS</td>
<td>.03</td>
<td>2004</td>
<td>
<table><tr><td style="text-align:left">.00</td><td>init</td><td></td></tr><tr><td style="text-align:left">4.21</td><td>LEFT</td><td>Fetched Schema</td></tr><tr><td style="text-align:left">2.18</td><td>TRANSFER</td><td>COMMON</td></tr><tr><td style="text-align:left">.03</td><td>RIGHT Schema Create</td><td>true</td></tr><tr><td style="text-align:left">.01</td><td>MSCK ran</td><td>true</td></tr></table></td>
<td>
<table><tr><td style="text-align:left">ALTER TABLE tpcds_bin_partitioned_orc_10.store_returns SET TBLPROPERTIES ("EXTERNAL"="true");</td></tr></table></td>
<td>
<table><tr><td style="text-align:left">ALTER TABLE tpcds_bin_partitioned_orc_10.store_returns SET TBLPROPERTIES ("external.table.purge"="true");</td></tr></table></td>
<td>
'discover.partitions'='true'<br/>'hmsMirror_Converted'='true'<br/>'hmsMirror_LegacyManaged'='true'<br/>'hmsMirror_Metadata_Stage1'='2021-05-11 18:38:39'<br/></td>
<td>
<ul><li>Schema was Legacy Managed or can be purged. The new schema does NOT have the external.table.purge' flag set.  This must be done manually.</li><li>DRY-RUN mode</li><li>DRY-RUN: MSCK NOT run</li><li>This table has partitions and is set for 'auto' discovery via table property 'discover.partitions'='true'. You've requested an immediate 'MSCK' for the table, so the partitions will be current. For future partition discovery, ensure the Metastore is running the 'PartitionManagementTask' service.</li></ul></td>
<td>
USE tpcds_bin_partitioned_orc_10;<br/><br/>CREATE EXTERNAL TABLE `store_returns`(<br/>  `sr_return_time_sk` bigint, <br/>  `sr_item_sk` bigint, <br/>  `sr_customer_sk` bigint, <br/>  `sr_cdemo_sk` bigint, <br/>  `sr_hdemo_sk` bigint, <br/>  `sr_addr_sk` bigint, <br/>  `sr_store_sk` bigint, <br/>  `sr_reason_sk` bigint, <br/>  `sr_ticket_number` bigint, <br/>  `sr_return_quantity` int, <br/>  `sr_return_amt` decimal(7,2), <br/>  `sr_return_tax` decimal(7,2), <br/>  `sr_return_amt_inc_tax` decimal(7,2), <br/>  `sr_fee` decimal(7,2), <br/>  `sr_return_ship_cost` decimal(7,2), <br/>  `sr_refunded_cash` decimal(7,2), <br/>  `sr_reversed_charge` decimal(7,2), <br/>  `sr_store_credit` decimal(7,2), <br/>  `sr_net_loss` decimal(7,2))<br/>PARTITIONED BY ( <br/>  `sr_returned_date_sk` bigint)<br/>ROW FORMAT SERDE <br/>  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' <br/>STORED AS INPUTFORMAT <br/>  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' <br/>OUTPUTFORMAT <br/>  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'<br/>LOCATION<br/>'hdfs://HDP50/apps/hive/warehouse/tpcds_bin_partitioned_orc_10.db/store_returns'<br/>TBLPROPERTIES (<br/>'hmsMirror_Metadata_Stage1'='2021-05-11 18:38:39',<br/>'hmsMirror_Converted'='true',<br/>'discover.partitions'='true',<br/>'hmsMirror_LegacyManaged'='true',<br/>  'transient_lastDdlTime'='1609881517');<br/><br/>MSCK REPAIR TABLE tpcds_bin_partitioned_orc_10.store_returns;<br/><br/></td>
</tr>
<tr>
<td>store_sales</td>
<td>SUCCESS</td>
<td>.00</td>
<td>1824</td>
<td>
<table><tr><td style="text-align:left">.00</td><td>init</td><td></td></tr><tr><td style="text-align:left">4.31</td><td>LEFT</td><td>Fetched Schema</td></tr><tr><td style="text-align:left">2.08</td><td>TRANSFER</td><td>COMMON</td></tr><tr><td style="text-align:left">.00</td><td>RIGHT Schema Create</td><td>true</td></tr><tr><td style="text-align:left">.00</td><td>MSCK ran</td><td>true</td></tr></table></td>
<td>
<table><tr><td style="text-align:left">ALTER TABLE tpcds_bin_partitioned_orc_10.store_sales SET TBLPROPERTIES ("EXTERNAL"="true");</td></tr></table></td>
<td>
<table><tr><td style="text-align:left">ALTER TABLE tpcds_bin_partitioned_orc_10.store_sales SET TBLPROPERTIES ("external.table.purge"="true");</td></tr></table></td>
<td>
'discover.partitions'='true'<br/>'hmsMirror_Converted'='true'<br/>'hmsMirror_LegacyManaged'='true'<br/>'hmsMirror_Metadata_Stage1'='2021-05-11 18:38:39'<br/></td>
<td>
<ul><li>Schema was Legacy Managed or can be purged. The new schema does NOT have the external.table.purge' flag set.  This must be done manually.</li><li>DRY-RUN mode</li><li>DRY-RUN: MSCK NOT run</li><li>This table has partitions and is set for 'auto' discovery via table property 'discover.partitions'='true'. You've requested an immediate 'MSCK' for the table, so the partitions will be current. For future partition discovery, ensure the Metastore is running the 'PartitionManagementTask' service.</li></ul></td>
<td>
USE tpcds_bin_partitioned_orc_10;<br/><br/>CREATE EXTERNAL TABLE `store_sales`(<br/>  `ss_sold_time_sk` bigint, <br/>  `ss_item_sk` bigint, <br/>  `ss_customer_sk` bigint, <br/>  `ss_cdemo_sk` bigint, <br/>  `ss_hdemo_sk` bigint, <br/>  `ss_addr_sk` bigint, <br/>  `ss_store_sk` bigint, <br/>  `ss_promo_sk` bigint, <br/>  `ss_ticket_number` bigint, <br/>  `ss_quantity` int, <br/>  `ss_wholesale_cost` decimal(7,2), <br/>  `ss_list_price` decimal(7,2), <br/>  `ss_sales_price` decimal(7,2), <br/>  `ss_ext_discount_amt` decimal(7,2), <br/>  `ss_ext_sales_price` decimal(7,2), <br/>  `ss_ext_wholesale_cost` decimal(7,2), <br/>  `ss_ext_list_price` decimal(7,2), <br/>  `ss_ext_tax` decimal(7,2), <br/>  `ss_coupon_amt` decimal(7,2), <br/>  `ss_net_paid` decimal(7,2), <br/>  `ss_net_paid_inc_tax` decimal(7,2), <br/>  `ss_net_profit` decimal(7,2))<br/>PARTITIONED BY ( <br/>  `ss_sold_date_sk` bigint)<br/>ROW FORMAT SERDE <br/>  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' <br/>STORED AS INPUTFORMAT <br/>  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' <br/>OUTPUTFORMAT <br/>  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'<br/>LOCATION<br/>'hdfs://HDP50/apps/hive/warehouse/tpcds_bin_partitioned_orc_10.db/store_sales'<br/>TBLPROPERTIES (<br/>'hmsMirror_Metadata_Stage1'='2021-05-11 18:38:39',<br/>'hmsMirror_Converted'='true',<br/>'discover.partitions'='true',<br/>'hmsMirror_LegacyManaged'='true',<br/>  'transient_lastDdlTime'='1609880834');<br/><br/>MSCK REPAIR TABLE tpcds_bin_partitioned_orc_10.store_sales;<br/><br/></td>
</tr>
<tr>
<td>time_dim</td>
<td>SUCCESS</td>
<td>.01</td>
<td> </td>
<td>
<table><tr><td style="text-align:left">.00</td><td>init</td><td></td></tr><tr><td style="text-align:left">4.39</td><td>LEFT</td><td>Fetched Schema</td></tr><tr><td style="text-align:left">2.00</td><td>TRANSFER</td><td>COMMON</td></tr><tr><td style="text-align:left">.01</td><td>RIGHT Schema Create</td><td>true</td></tr></table></td>
<td>
<table><tr><td style="text-align:left">ALTER TABLE tpcds_bin_partitioned_orc_10.time_dim SET TBLPROPERTIES ("EXTERNAL"="true");</td></tr></table></td>
<td>
<table><tr><td style="text-align:left">ALTER TABLE tpcds_bin_partitioned_orc_10.time_dim SET TBLPROPERTIES ("external.table.purge"="true");</td></tr></table></td>
<td>
'discover.partitions'='true'<br/>'hmsMirror_Converted'='true'<br/>'hmsMirror_LegacyManaged'='true'<br/>'hmsMirror_Metadata_Stage1'='2021-05-11 18:38:39'<br/></td>
<td>
<ul><li>Schema was Legacy Managed or can be purged. The new schema does NOT have the external.table.purge' flag set.  This must be done manually.</li><li>DRY-RUN mode</li></ul></td>
<td>
USE tpcds_bin_partitioned_orc_10;<br/><br/>CREATE EXTERNAL TABLE `time_dim`(<br/>  `t_time_sk` bigint, <br/>  `t_time_id` char(16), <br/>  `t_time` int, <br/>  `t_hour` int, <br/>  `t_minute` int, <br/>  `t_second` int, <br/>  `t_am_pm` char(2), <br/>  `t_shift` char(20), <br/>  `t_sub_shift` char(20), <br/>  `t_meal_time` char(20))<br/>ROW FORMAT SERDE <br/>  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' <br/>STORED AS INPUTFORMAT <br/>  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' <br/>OUTPUTFORMAT <br/>  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'<br/>LOCATION<br/>'hdfs://HDP50/apps/hive/warehouse/tpcds_bin_partitioned_orc_10.db/time_dim'<br/>TBLPROPERTIES (<br/>'hmsMirror_Metadata_Stage1'='2021-05-11 18:38:39',<br/>'hmsMirror_Converted'='true',<br/>'discover.partitions'='true',<br/>'hmsMirror_LegacyManaged'='true',<br/>  'transient_lastDdlTime'='1609880505');<br/><br/></td>
</tr>
<tr>
<td>warehouse</td>
<td>SUCCESS</td>
<td>.02</td>
<td> </td>
<td>
<table><tr><td style="text-align:left">.00</td><td>init</td><td></td></tr><tr><td style="text-align:left">4.48</td><td>LEFT</td><td>Fetched Schema</td></tr><tr><td style="text-align:left">1.91</td><td>TRANSFER</td><td>COMMON</td></tr><tr><td style="text-align:left">.01</td><td>RIGHT Schema Create</td><td>true</td></tr></table></td>
<td>
<table><tr><td style="text-align:left">ALTER TABLE tpcds_bin_partitioned_orc_10.warehouse SET TBLPROPERTIES ("EXTERNAL"="true");</td></tr></table></td>
<td>
<table><tr><td style="text-align:left">ALTER TABLE tpcds_bin_partitioned_orc_10.warehouse SET TBLPROPERTIES ("external.table.purge"="true");</td></tr></table></td>
<td>
'discover.partitions'='true'<br/>'hmsMirror_Converted'='true'<br/>'hmsMirror_LegacyManaged'='true'<br/>'hmsMirror_Metadata_Stage1'='2021-05-11 18:38:39'<br/></td>
<td>
<ul><li>Schema was Legacy Managed or can be purged. The new schema does NOT have the external.table.purge' flag set.  This must be done manually.</li><li>DRY-RUN mode</li></ul></td>
<td>
USE tpcds_bin_partitioned_orc_10;<br/><br/>CREATE EXTERNAL TABLE `warehouse`(<br/>  `w_warehouse_sk` bigint, <br/>  `w_warehouse_id` char(16), <br/>  `w_warehouse_name` varchar(20), <br/>  `w_warehouse_sq_ft` int, <br/>  `w_street_number` char(10), <br/>  `w_street_name` varchar(60), <br/>  `w_street_type` char(15), <br/>  `w_suite_number` char(10), <br/>  `w_city` varchar(60), <br/>  `w_county` varchar(30), <br/>  `w_state` char(2), <br/>  `w_zip` char(10), <br/>  `w_country` varchar(20), <br/>  `w_gmt_offset` decimal(5,2))<br/>ROW FORMAT SERDE <br/>  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' <br/>STORED AS INPUTFORMAT <br/>  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' <br/>OUTPUTFORMAT <br/>  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'<br/>LOCATION<br/>'hdfs://HDP50/apps/hive/warehouse/tpcds_bin_partitioned_orc_10.db/warehouse'<br/>TBLPROPERTIES (<br/>'hmsMirror_Metadata_Stage1'='2021-05-11 18:38:39',<br/>'hmsMirror_Converted'='true',<br/>'discover.partitions'='true',<br/>'hmsMirror_LegacyManaged'='true',<br/>  'transient_lastDdlTime'='1609880689');<br/><br/></td>
</tr>
<tr>
<td>web_page</td>
<td>SUCCESS</td>
<td>.03</td>
<td> </td>
<td>
<table><tr><td style="text-align:left">.00</td><td>init</td><td></td></tr><tr><td style="text-align:left">4.76</td><td>LEFT</td><td>Fetched Schema</td></tr><tr><td style="text-align:left">1.64</td><td>TRANSFER</td><td>COMMON</td></tr><tr><td style="text-align:left">.02</td><td>RIGHT Schema Create</td><td>true</td></tr></table></td>
<td>
<table><tr><td style="text-align:left">ALTER TABLE tpcds_bin_partitioned_orc_10.web_page SET TBLPROPERTIES ("EXTERNAL"="true");</td></tr></table></td>
<td>
<table><tr><td style="text-align:left">ALTER TABLE tpcds_bin_partitioned_orc_10.web_page SET TBLPROPERTIES ("external.table.purge"="true");</td></tr></table></td>
<td>
'discover.partitions'='true'<br/>'hmsMirror_Converted'='true'<br/>'hmsMirror_LegacyManaged'='true'<br/>'hmsMirror_Metadata_Stage1'='2021-05-11 18:38:39'<br/></td>
<td>
<ul><li>Schema was Legacy Managed or can be purged. The new schema does NOT have the external.table.purge' flag set.  This must be done manually.</li><li>DRY-RUN mode</li></ul></td>
<td>
USE tpcds_bin_partitioned_orc_10;<br/><br/>CREATE EXTERNAL TABLE `web_page`(<br/>  `wp_web_page_sk` bigint, <br/>  `wp_web_page_id` char(16), <br/>  `wp_rec_start_date` date, <br/>  `wp_rec_end_date` date, <br/>  `wp_creation_date_sk` bigint, <br/>  `wp_access_date_sk` bigint, <br/>  `wp_autogen_flag` char(1), <br/>  `wp_customer_sk` bigint, <br/>  `wp_url` varchar(100), <br/>  `wp_type` char(50), <br/>  `wp_char_count` int, <br/>  `wp_link_count` int, <br/>  `wp_image_count` int, <br/>  `wp_max_ad_count` int)<br/>ROW FORMAT SERDE <br/>  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' <br/>STORED AS INPUTFORMAT <br/>  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' <br/>OUTPUTFORMAT <br/>  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'<br/>LOCATION<br/>'hdfs://HDP50/apps/hive/warehouse/tpcds_bin_partitioned_orc_10.db/web_page'<br/>TBLPROPERTIES (<br/>'hmsMirror_Metadata_Stage1'='2021-05-11 18:38:39',<br/>'hmsMirror_Converted'='true',<br/>'discover.partitions'='true',<br/>'hmsMirror_LegacyManaged'='true',<br/>  'transient_lastDdlTime'='1609880787');<br/><br/></td>
</tr>
<tr>
<td>web_returns</td>
<td>SUCCESS</td>
<td>.00</td>
<td>2166</td>
<td>
<table><tr><td style="text-align:left">.00</td><td>init</td><td></td></tr><tr><td style="text-align:left">4.85</td><td>LEFT</td><td>Fetched Schema</td></tr><tr><td style="text-align:left">1.55</td><td>TRANSFER</td><td>COMMON</td></tr><tr><td style="text-align:left">.00</td><td>RIGHT Schema Create</td><td>true</td></tr><tr><td style="text-align:left">.00</td><td>MSCK ran</td><td>true</td></tr></table></td>
<td>
<table><tr><td style="text-align:left">ALTER TABLE tpcds_bin_partitioned_orc_10.web_returns SET TBLPROPERTIES ("EXTERNAL"="true");</td></tr></table></td>
<td>
<table><tr><td style="text-align:left">ALTER TABLE tpcds_bin_partitioned_orc_10.web_returns SET TBLPROPERTIES ("external.table.purge"="true");</td></tr></table></td>
<td>
'discover.partitions'='true'<br/>'hmsMirror_Converted'='true'<br/>'hmsMirror_LegacyManaged'='true'<br/>'hmsMirror_Metadata_Stage1'='2021-05-11 18:38:39'<br/></td>
<td>
<ul><li>Schema was Legacy Managed or can be purged. The new schema does NOT have the external.table.purge' flag set.  This must be done manually.</li><li>DRY-RUN mode</li><li>DRY-RUN: MSCK NOT run</li><li>This table has partitions and is set for 'auto' discovery via table property 'discover.partitions'='true'. You've requested an immediate 'MSCK' for the table, so the partitions will be current. For future partition discovery, ensure the Metastore is running the 'PartitionManagementTask' service.</li></ul></td>
<td>
USE tpcds_bin_partitioned_orc_10;<br/><br/>CREATE EXTERNAL TABLE `web_returns`(<br/>  `wr_returned_time_sk` bigint, <br/>  `wr_item_sk` bigint, <br/>  `wr_refunded_customer_sk` bigint, <br/>  `wr_refunded_cdemo_sk` bigint, <br/>  `wr_refunded_hdemo_sk` bigint, <br/>  `wr_refunded_addr_sk` bigint, <br/>  `wr_returning_customer_sk` bigint, <br/>  `wr_returning_cdemo_sk` bigint, <br/>  `wr_returning_hdemo_sk` bigint, <br/>  `wr_returning_addr_sk` bigint, <br/>  `wr_web_page_sk` bigint, <br/>  `wr_reason_sk` bigint, <br/>  `wr_order_number` bigint, <br/>  `wr_return_quantity` int, <br/>  `wr_return_amt` decimal(7,2), <br/>  `wr_return_tax` decimal(7,2), <br/>  `wr_return_amt_inc_tax` decimal(7,2), <br/>  `wr_fee` decimal(7,2), <br/>  `wr_return_ship_cost` decimal(7,2), <br/>  `wr_refunded_cash` decimal(7,2), <br/>  `wr_reversed_charge` decimal(7,2), <br/>  `wr_account_credit` decimal(7,2), <br/>  `wr_net_loss` decimal(7,2))<br/>PARTITIONED BY ( <br/>  `wr_returned_date_sk` bigint)<br/>ROW FORMAT SERDE <br/>  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' <br/>STORED AS INPUTFORMAT <br/>  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' <br/>OUTPUTFORMAT <br/>  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'<br/>LOCATION<br/>'hdfs://HDP50/apps/hive/warehouse/tpcds_bin_partitioned_orc_10.db/web_returns'<br/>TBLPROPERTIES (<br/>'hmsMirror_Metadata_Stage1'='2021-05-11 18:38:39',<br/>'hmsMirror_Converted'='true',<br/>'discover.partitions'='true',<br/>'hmsMirror_LegacyManaged'='true',<br/>  'transient_lastDdlTime'='1609882861');<br/><br/>MSCK REPAIR TABLE tpcds_bin_partitioned_orc_10.web_returns;<br/><br/></td>
</tr>
<tr>
<td>web_sales</td>
<td>SUCCESS</td>
<td>.01</td>
<td>1824</td>
<td>
<table><tr><td style="text-align:left">.00</td><td>init</td><td></td></tr><tr><td style="text-align:left">4.94</td><td>LEFT</td><td>Fetched Schema</td></tr><tr><td style="text-align:left">1.46</td><td>TRANSFER</td><td>COMMON</td></tr><tr><td style="text-align:left">.00</td><td>RIGHT Schema Create</td><td>true</td></tr><tr><td style="text-align:left">.00</td><td>MSCK ran</td><td>true</td></tr></table></td>
<td>
<table><tr><td style="text-align:left">ALTER TABLE tpcds_bin_partitioned_orc_10.web_sales SET TBLPROPERTIES ("EXTERNAL"="true");</td></tr></table></td>
<td>
<table><tr><td style="text-align:left">ALTER TABLE tpcds_bin_partitioned_orc_10.web_sales SET TBLPROPERTIES ("external.table.purge"="true");</td></tr></table></td>
<td>
'discover.partitions'='true'<br/>'hmsMirror_Converted'='true'<br/>'hmsMirror_LegacyManaged'='true'<br/>'hmsMirror_Metadata_Stage1'='2021-05-11 18:38:39'<br/></td>
<td>
<ul><li>Schema was Legacy Managed or can be purged. The new schema does NOT have the external.table.purge' flag set.  This must be done manually.</li><li>DRY-RUN mode</li><li>DRY-RUN: MSCK NOT run</li><li>This table has partitions and is set for 'auto' discovery via table property 'discover.partitions'='true'. You've requested an immediate 'MSCK' for the table, so the partitions will be current. For future partition discovery, ensure the Metastore is running the 'PartitionManagementTask' service.</li></ul></td>
<td>
USE tpcds_bin_partitioned_orc_10;<br/><br/>CREATE EXTERNAL TABLE `web_sales`(<br/>  `ws_sold_time_sk` bigint, <br/>  `ws_ship_date_sk` bigint, <br/>  `ws_item_sk` bigint, <br/>  `ws_bill_customer_sk` bigint, <br/>  `ws_bill_cdemo_sk` bigint, <br/>  `ws_bill_hdemo_sk` bigint, <br/>  `ws_bill_addr_sk` bigint, <br/>  `ws_ship_customer_sk` bigint, <br/>  `ws_ship_cdemo_sk` bigint, <br/>  `ws_ship_hdemo_sk` bigint, <br/>  `ws_ship_addr_sk` bigint, <br/>  `ws_web_page_sk` bigint, <br/>  `ws_web_site_sk` bigint, <br/>  `ws_ship_mode_sk` bigint, <br/>  `ws_warehouse_sk` bigint, <br/>  `ws_promo_sk` bigint, <br/>  `ws_order_number` bigint, <br/>  `ws_quantity` int, <br/>  `ws_wholesale_cost` decimal(7,2), <br/>  `ws_list_price` decimal(7,2), <br/>  `ws_sales_price` decimal(7,2), <br/>  `ws_ext_discount_amt` decimal(7,2), <br/>  `ws_ext_sales_price` decimal(7,2), <br/>  `ws_ext_wholesale_cost` decimal(7,2), <br/>  `ws_ext_list_price` decimal(7,2), <br/>  `ws_ext_tax` decimal(7,2), <br/>  `ws_coupon_amt` decimal(7,2), <br/>  `ws_ext_ship_cost` decimal(7,2), <br/>  `ws_net_paid` decimal(7,2), <br/>  `ws_net_paid_inc_tax` decimal(7,2), <br/>  `ws_net_paid_inc_ship` decimal(7,2), <br/>  `ws_net_paid_inc_ship_tax` decimal(7,2), <br/>  `ws_net_profit` decimal(7,2))<br/>PARTITIONED BY ( <br/>  `ws_sold_date_sk` bigint)<br/>ROW FORMAT SERDE <br/>  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' <br/>STORED AS INPUTFORMAT <br/>  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' <br/>OUTPUTFORMAT <br/>  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'<br/>LOCATION<br/>'hdfs://HDP50/apps/hive/warehouse/tpcds_bin_partitioned_orc_10.db/web_sales'<br/>TBLPROPERTIES (<br/>'hmsMirror_Metadata_Stage1'='2021-05-11 18:38:39',<br/>'hmsMirror_Converted'='true',<br/>'discover.partitions'='true',<br/>'hmsMirror_LegacyManaged'='true',<br/>  'transient_lastDdlTime'='1609882251');<br/><br/>MSCK REPAIR TABLE tpcds_bin_partitioned_orc_10.web_sales;<br/><br/></td>
</tr>
<tr>
<td>web_site</td>
<td>SUCCESS</td>
<td>.00</td>
<td> </td>
<td>
<table><tr><td style="text-align:left">.00</td><td>init</td><td></td></tr><tr><td style="text-align:left">5.04</td><td>LEFT</td><td>Fetched Schema</td></tr><tr><td style="text-align:left">1.37</td><td>TRANSFER</td><td>COMMON</td></tr><tr><td style="text-align:left">.00</td><td>RIGHT Schema Create</td><td>true</td></tr></table></td>
<td>
<table><tr><td style="text-align:left">ALTER TABLE tpcds_bin_partitioned_orc_10.web_site SET TBLPROPERTIES ("EXTERNAL"="true");</td></tr></table></td>
<td>
<table><tr><td style="text-align:left">ALTER TABLE tpcds_bin_partitioned_orc_10.web_site SET TBLPROPERTIES ("external.table.purge"="true");</td></tr></table></td>
<td>
'discover.partitions'='true'<br/>'hmsMirror_Converted'='true'<br/>'hmsMirror_LegacyManaged'='true'<br/>'hmsMirror_Metadata_Stage1'='2021-05-11 18:38:39'<br/></td>
<td>
<ul><li>Schema was Legacy Managed or can be purged. The new schema does NOT have the external.table.purge' flag set.  This must be done manually.</li><li>DRY-RUN mode</li></ul></td>
<td>
USE tpcds_bin_partitioned_orc_10;<br/><br/>CREATE EXTERNAL TABLE `web_site`(<br/>  `web_site_sk` bigint, <br/>  `web_site_id` char(16), <br/>  `web_rec_start_date` date, <br/>  `web_rec_end_date` date, <br/>  `web_name` varchar(50), <br/>  `web_open_date_sk` bigint, <br/>  `web_close_date_sk` bigint, <br/>  `web_class` varchar(50), <br/>  `web_manager` varchar(40), <br/>  `web_mkt_id` int, <br/>  `web_mkt_class` varchar(50), <br/>  `web_mkt_desc` varchar(100), <br/>  `web_market_manager` varchar(40), <br/>  `web_company_id` int, <br/>  `web_company_name` char(50), <br/>  `web_street_number` char(10), <br/>  `web_street_name` varchar(60), <br/>  `web_street_type` char(15), <br/>  `web_suite_number` char(10), <br/>  `web_city` varchar(60), <br/>  `web_county` varchar(30), <br/>  `web_state` char(2), <br/>  `web_zip` char(10), <br/>  `web_country` varchar(20), <br/>  `web_gmt_offset` decimal(5,2), <br/>  `web_tax_percentage` decimal(5,2))<br/>ROW FORMAT SERDE <br/>  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' <br/>STORED AS INPUTFORMAT <br/>  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' <br/>OUTPUTFORMAT <br/>  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'<br/>LOCATION<br/>'hdfs://HDP50/apps/hive/warehouse/tpcds_bin_partitioned_orc_10.db/web_site'<br/>TBLPROPERTIES (<br/>'hmsMirror_Metadata_Stage1'='2021-05-11 18:38:39',<br/>'hmsMirror_Converted'='true',<br/>'discover.partitions'='true',<br/>'hmsMirror_LegacyManaged'='true',<br/>  'transient_lastDdlTime'='1609880830');<br/><br/></td>
</tr>
</table>
