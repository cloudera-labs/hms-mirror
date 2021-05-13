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
ALTER TABLE tpcds_bin_partitioned_orc_10.catalog_returns SET TBLPROPERTIES ("external.table.purge"="true");

--    Table: catalog_sales
ALTER TABLE tpcds_bin_partitioned_orc_10.catalog_sales SET TBLPROPERTIES ("external.table.purge"="true");

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
ALTER TABLE tpcds_bin_partitioned_orc_10.store_returns SET TBLPROPERTIES ("external.table.purge"="true");

--    Table: store_sales
ALTER TABLE tpcds_bin_partitioned_orc_10.store_sales SET TBLPROPERTIES ("external.table.purge"="true");

--    Table: time_dim
ALTER TABLE tpcds_bin_partitioned_orc_10.time_dim SET TBLPROPERTIES ("external.table.purge"="true");

--    Table: warehouse
ALTER TABLE tpcds_bin_partitioned_orc_10.warehouse SET TBLPROPERTIES ("external.table.purge"="true");

--    Table: web_page
ALTER TABLE tpcds_bin_partitioned_orc_10.web_page SET TBLPROPERTIES ("external.table.purge"="true");

--    Table: web_returns
ALTER TABLE tpcds_bin_partitioned_orc_10.web_returns SET TBLPROPERTIES ("external.table.purge"="true");

--    Table: web_sales
ALTER TABLE tpcds_bin_partitioned_orc_10.web_sales SET TBLPROPERTIES ("external.table.purge"="true");

--    Table: web_site
ALTER TABLE tpcds_bin_partitioned_orc_10.web_site SET TBLPROPERTIES ("external.table.purge"="true");

