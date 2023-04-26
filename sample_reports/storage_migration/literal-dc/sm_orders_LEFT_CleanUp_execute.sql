-- EXECUTION CLEANUP script for sm_orders on LEFT cluster

-- 2023-04-26_12-05-53

USE sm_orders;

--    Cleanup script: order_item_orc
DROP TABLE IF EXISTS order_item_orc_b1af29dcb3d84091a597161c999046c6;

--    Cleanup script: order_item_small_orc
DROP TABLE IF EXISTS order_item_small_orc_ac3a68adf0304b2e8614ecedd119c20c;

--    Cleanup script: order_orc
DROP TABLE IF EXISTS order_orc_2d560d516fa84983a96970e252f6a4b0;

--    Cleanup script: order_small_orc
DROP TABLE IF EXISTS order_small_orc_14fa588056ea488c9804b760550a747e;

--    Cleanup script: order_src
DROP TABLE IF EXISTS order_src_d5a6b71c3287401e80b88abbf3ee44f3;
