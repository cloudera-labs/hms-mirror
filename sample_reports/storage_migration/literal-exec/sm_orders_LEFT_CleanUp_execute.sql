-- EXECUTION CLEANUP script for sm_orders on LEFT cluster

-- 2023-04-26_12-08-09

USE sm_orders;

--    Cleanup script: mngd_order_item_orc
DROP TABLE IF EXISTS mngd_order_item_orc_f0ea64e31cf147edb4afbd4bbe2eafa5;

--    Cleanup script: mngd_order_item_small_orc
DROP TABLE IF EXISTS mngd_order_item_small_orc_6a146a8ae0384cc09786ff248e659bf7;

--    Cleanup script: mngd_order_orc
DROP TABLE IF EXISTS mngd_order_orc_66603a32bb344a4c86b623fa0b808703;

--    Cleanup script: mngd_order_small_orc
DROP TABLE IF EXISTS mngd_order_small_orc_3bf1ff616b2e4e5fadddaec689a22d75;

--    Cleanup script: order_item_orc
DROP TABLE IF EXISTS order_item_orc_1e592260cbae4600b8f6c24bb168b057;

--    Cleanup script: order_item_small_orc
DROP TABLE IF EXISTS order_item_small_orc_f8412906c3994f88b1673264fd7b42f1;

--    Cleanup script: order_orc
DROP TABLE IF EXISTS order_orc_525c9296e82142ada064dfeb9306a9f9;

--    Cleanup script: order_small_orc
DROP TABLE IF EXISTS order_small_orc_035800882a7c44c8996d7e552a6cb327;

--    Cleanup script: order_src
DROP TABLE IF EXISTS order_src_1fb2108275ce44b0a12bf0a38f7cc092;
