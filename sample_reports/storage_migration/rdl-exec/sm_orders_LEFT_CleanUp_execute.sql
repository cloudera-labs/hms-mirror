-- EXECUTION CLEANUP script for sm_orders on LEFT cluster

-- 2023-04-26_12-10-21

USE sm_orders;

--    Cleanup script: mngd_order_item_orc
DROP TABLE IF EXISTS mngd_order_item_orc_bba5f0beaa394d59aa64f2d715939461;

--    Cleanup script: mngd_order_item_small_orc
DROP TABLE IF EXISTS mngd_order_item_small_orc_e61247eaaea945e1b332ad9ba42cc532;

--    Cleanup script: mngd_order_orc
DROP TABLE IF EXISTS mngd_order_orc_261cc27a86dd4ab2b1a7ac38393c431e;

--    Cleanup script: mngd_order_small_orc
DROP TABLE IF EXISTS mngd_order_small_orc_87fcf4d70fe644408b82ee5376d226f1;

--    Cleanup script: order_item_orc
DROP TABLE IF EXISTS order_item_orc_d423fe9a47a14c8a9bbab655c3649c63;

--    Cleanup script: order_item_small_orc
DROP TABLE IF EXISTS order_item_small_orc_a5381521ed5d45febaddc9f0e43c8963;

--    Cleanup script: order_orc
DROP TABLE IF EXISTS order_orc_20b2758e3f4441d487c4061372480263;

--    Cleanup script: order_small_orc
DROP TABLE IF EXISTS order_small_orc_eb7078c838524dd79d0e6ced6500a309;

--    Cleanup script: order_src
DROP TABLE IF EXISTS order_src_95865504ddaf4c15ada73081e74117b7;
