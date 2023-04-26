-- EXECUTION CLEANUP script for sm_orders on LEFT cluster

-- 2023-04-26_12-07-03

USE sm_orders;

--    Cleanup script: mngd_order_item_orc
DROP TABLE IF EXISTS mngd_order_item_orc_b723de0a4008437095141f39c24349ec;

--    Cleanup script: mngd_order_item_small_orc
DROP TABLE IF EXISTS mngd_order_item_small_orc_8108bc20c7ef4fc59611d25459fecf63;

--    Cleanup script: mngd_order_orc
DROP TABLE IF EXISTS mngd_order_orc_8e63f5ebb9394e12a77f4e3f62eb3b52;

--    Cleanup script: mngd_order_small_orc
DROP TABLE IF EXISTS mngd_order_small_orc_be1610dd2d4a43128d9b1adf12cddce8;

--    Cleanup script: order_item_orc
DROP TABLE IF EXISTS order_item_orc_865f14c0bebf486eaa83ed418e886abf;

--    Cleanup script: order_item_small_orc
DROP TABLE IF EXISTS order_item_small_orc_fbd2ba431f0947f1b20165e2671a053b;

--    Cleanup script: order_orc
DROP TABLE IF EXISTS order_orc_bac0df8d058742b9ae1c6295d080be80;

--    Cleanup script: order_small_orc
DROP TABLE IF EXISTS order_small_orc_43a1edde205d4e338fa6502b2b0f5fe8;

--    Cleanup script: order_src
DROP TABLE IF EXISTS order_src_b14312a499594ec1811ef13b3a687d2c;
