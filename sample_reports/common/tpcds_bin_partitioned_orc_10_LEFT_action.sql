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

-- ACTION script for LEFT cluster

-- HELPER Script to assist with MANUAL updates.
-- RUN AT OWN RISK  !!!
-- REVIEW and UNDERSTAND the adjustments below before running.

-- DATABASE: tpcds_bin_partitioned_orc_10
--    Table: call_center
ALTER TABLE tpcds_bin_partitioned_orc_10.call_center SET TBLPROPERTIES ("EXTERNAL"="true");

--    Table: catalog_page
ALTER TABLE tpcds_bin_partitioned_orc_10.catalog_page SET TBLPROPERTIES ("EXTERNAL"="true");

--    Table: catalog_returns
ALTER TABLE tpcds_bin_partitioned_orc_10.catalog_returns SET TBLPROPERTIES ("EXTERNAL"="true");

--    Table: catalog_sales
ALTER TABLE tpcds_bin_partitioned_orc_10.catalog_sales SET TBLPROPERTIES ("EXTERNAL"="true");

--    Table: customer
ALTER TABLE tpcds_bin_partitioned_orc_10.customer SET TBLPROPERTIES ("EXTERNAL"="true");

--    Table: customer_address
ALTER TABLE tpcds_bin_partitioned_orc_10.customer_address SET TBLPROPERTIES ("EXTERNAL"="true");

--    Table: customer_demographics
ALTER TABLE tpcds_bin_partitioned_orc_10.customer_demographics SET TBLPROPERTIES ("EXTERNAL"="true");

--    Table: date_dim
ALTER TABLE tpcds_bin_partitioned_orc_10.date_dim SET TBLPROPERTIES ("EXTERNAL"="true");

--    Table: household_demographics
ALTER TABLE tpcds_bin_partitioned_orc_10.household_demographics SET TBLPROPERTIES ("EXTERNAL"="true");

--    Table: income_band
ALTER TABLE tpcds_bin_partitioned_orc_10.income_band SET TBLPROPERTIES ("EXTERNAL"="true");

--    Table: inventory
ALTER TABLE tpcds_bin_partitioned_orc_10.inventory SET TBLPROPERTIES ("EXTERNAL"="true");

--    Table: item
ALTER TABLE tpcds_bin_partitioned_orc_10.item SET TBLPROPERTIES ("EXTERNAL"="true");

--    Table: promotion
ALTER TABLE tpcds_bin_partitioned_orc_10.promotion SET TBLPROPERTIES ("EXTERNAL"="true");

--    Table: reason
ALTER TABLE tpcds_bin_partitioned_orc_10.reason SET TBLPROPERTIES ("EXTERNAL"="true");

--    Table: ship_mode
ALTER TABLE tpcds_bin_partitioned_orc_10.ship_mode SET TBLPROPERTIES ("EXTERNAL"="true");

--    Table: store
ALTER TABLE tpcds_bin_partitioned_orc_10.store SET TBLPROPERTIES ("EXTERNAL"="true");

--    Table: store_returns
ALTER TABLE tpcds_bin_partitioned_orc_10.store_returns SET TBLPROPERTIES ("EXTERNAL"="true");

--    Table: store_sales
ALTER TABLE tpcds_bin_partitioned_orc_10.store_sales SET TBLPROPERTIES ("EXTERNAL"="true");

--    Table: time_dim
ALTER TABLE tpcds_bin_partitioned_orc_10.time_dim SET TBLPROPERTIES ("EXTERNAL"="true");

--    Table: warehouse
ALTER TABLE tpcds_bin_partitioned_orc_10.warehouse SET TBLPROPERTIES ("EXTERNAL"="true");

--    Table: web_page
ALTER TABLE tpcds_bin_partitioned_orc_10.web_page SET TBLPROPERTIES ("EXTERNAL"="true");

--    Table: web_returns
ALTER TABLE tpcds_bin_partitioned_orc_10.web_returns SET TBLPROPERTIES ("EXTERNAL"="true");

--    Table: web_sales
ALTER TABLE tpcds_bin_partitioned_orc_10.web_sales SET TBLPROPERTIES ("EXTERNAL"="true");

--    Table: web_site
ALTER TABLE tpcds_bin_partitioned_orc_10.web_site SET TBLPROPERTIES ("EXTERNAL"="true");

