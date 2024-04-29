-- EXECUTION CLEANUP script for sm_orders on LEFT cluster

-- 2023-04-26_12-09-32

USE sm_orders;

--    Cleanup script: mngd_order_item_orc
DROP TABLE IF EXISTS mngd_order_item_orc_bc9c89a2fc7f4fd2a64402869b97ca42;

--    Cleanup script: mngd_order_item_small_orc
DROP TABLE IF EXISTS mngd_order_item_small_orc_b028df3715c541d784dda9deb29936c3;

--    Cleanup script: mngd_order_orc
DROP TABLE IF EXISTS mngd_order_orc_da7c308560bd4d0a8790d1f7b68b64cc;

--    Cleanup script: mngd_order_small_orc
DROP TABLE IF EXISTS mngd_order_small_orc_640778cfae4e4b9182db8d4904ca936a;

--    Cleanup script: order_item_orc
DROP TABLE IF EXISTS order_item_orc_e2b738365a514daa94500b657cc1350c;

--    Cleanup script: order_item_small_orc
DROP TABLE IF EXISTS order_item_small_orc_ced67c4c3fbc498b8018c39b5b567068;

--    Cleanup script: order_orc
DROP TABLE IF EXISTS order_orc_6e69c0d258c14399ac65323ca435fed5;

--    Cleanup script: order_small_orc
DROP TABLE IF EXISTS order_small_orc_4fdbbb7e60644e15b5cc690910d7457f;

--    Cleanup script: order_src
DROP TABLE IF EXISTS order_src_869b10b9a09e49068dd418509f4febca;
