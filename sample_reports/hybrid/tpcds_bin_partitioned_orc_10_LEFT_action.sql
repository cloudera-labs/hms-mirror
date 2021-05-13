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

--    Table: catalog_sales

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

--    Table: store_sales

--    Table: time_dim
ALTER TABLE tpcds_bin_partitioned_orc_10.time_dim SET TBLPROPERTIES ("EXTERNAL"="true");

--    Table: warehouse
ALTER TABLE tpcds_bin_partitioned_orc_10.warehouse SET TBLPROPERTIES ("EXTERNAL"="true");

--    Table: web_page
ALTER TABLE tpcds_bin_partitioned_orc_10.web_page SET TBLPROPERTIES ("EXTERNAL"="true");

--    Table: web_returns

--    Table: web_sales

--    Table: web_site
ALTER TABLE tpcds_bin_partitioned_orc_10.web_site SET TBLPROPERTIES ("EXTERNAL"="true");

