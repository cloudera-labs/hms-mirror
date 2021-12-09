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

-- ACTION script for RIGHT cluster

-- HELPER Script to assist with MANUAL updates.
-- RUN AT OWN RISK  !!!
-- REVIEW and UNDERSTAND the adjustments below before running.

-- DATABASE: tpcds_bin_partitioned_orc_10
--    Table: call_center
ALTER TABLE tpcds_bin_partitioned_orc_10.call_center SET TBLPROPERTIES ("external.table.purge"="true");

--    Table: catalog_page
ALTER TABLE tpcds_bin_partitioned_orc_10.catalog_page SET TBLPROPERTIES ("external.table.purge"="true");

--    Table: catalog_returns

--    Table: catalog_sales

--    Table: customer
ALTER TABLE tpcds_bin_partitioned_orc_10.customer SET TBLPROPERTIES ("external.table.purge"="true");

--    Table: customer_address
ALTER TABLE tpcds_bin_partitioned_orc_10.customer_address SET TBLPROPERTIES ("external.table.purge"="true");

--    Table: customer_demographics
ALTER TABLE tpcds_bin_partitioned_orc_10.customer_demographics SET TBLPROPERTIES ("external.table.purge"="true");

--    Table: date_dim
ALTER TABLE tpcds_bin_partitioned_orc_10.date_dim SET TBLPROPERTIES ("external.table.purge"="true");

--    Table: household_demographics
ALTER TABLE tpcds_bin_partitioned_orc_10.household_demographics SET TBLPROPERTIES ("external.table.purge"="true");

--    Table: income_band
ALTER TABLE tpcds_bin_partitioned_orc_10.income_band SET TBLPROPERTIES ("external.table.purge"="true");

--    Table: inventory
ALTER TABLE tpcds_bin_partitioned_orc_10.inventory SET TBLPROPERTIES ("external.table.purge"="true");

--    Table: item
ALTER TABLE tpcds_bin_partitioned_orc_10.item SET TBLPROPERTIES ("external.table.purge"="true");

--    Table: promotion
ALTER TABLE tpcds_bin_partitioned_orc_10.promotion SET TBLPROPERTIES ("external.table.purge"="true");

--    Table: reason
ALTER TABLE tpcds_bin_partitioned_orc_10.reason SET TBLPROPERTIES ("external.table.purge"="true");

--    Table: ship_mode
ALTER TABLE tpcds_bin_partitioned_orc_10.ship_mode SET TBLPROPERTIES ("external.table.purge"="true");

--    Table: store
ALTER TABLE tpcds_bin_partitioned_orc_10.store SET TBLPROPERTIES ("external.table.purge"="true");

--    Table: store_returns

--    Table: store_sales

--    Table: time_dim
ALTER TABLE tpcds_bin_partitioned_orc_10.time_dim SET TBLPROPERTIES ("external.table.purge"="true");

--    Table: warehouse
ALTER TABLE tpcds_bin_partitioned_orc_10.warehouse SET TBLPROPERTIES ("external.table.purge"="true");

--    Table: web_page
ALTER TABLE tpcds_bin_partitioned_orc_10.web_page SET TBLPROPERTIES ("external.table.purge"="true");

--    Table: web_returns

--    Table: web_sales

--    Table: web_site
ALTER TABLE tpcds_bin_partitioned_orc_10.web_site SET TBLPROPERTIES ("external.table.purge"="true");

