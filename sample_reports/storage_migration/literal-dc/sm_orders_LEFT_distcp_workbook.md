## WARNING
Using the options `-dc` and `-rdl` together may yield some inconsistent results. __If the 'current' table locations don't match the table name__, `distcp` will NOT realign those directories to the table names.  Which means the adjusted tables may not align with the directories. See: [Issue #35](https://github.com/cloudera-labs/hms-mirror/issues/35) for work going on to address this.

| Database | Target | Sources |
|:---|:---|:---|
| sm_orders | | |
| | ofs://OHOME90/user/dstreev/datasets | hdfs://HOME90/user/dstreev/datasets/orders_small<br> | 
| | ofs://OHOME90/warehouse/tablespace/external/hive/sm_orders.db | hdfs://HOME90/warehouse/tablespace/external/hive/sm_orders.db/order_item_orc<br>hdfs://HOME90/warehouse/tablespace/external/hive/sm_orders.db/order_item_small_orc<br>hdfs://HOME90/warehouse/tablespace/external/hive/sm_orders.db/order_orc<br>hdfs://HOME90/warehouse/tablespace/external/hive/sm_orders.db/order_small_orc<br> | 
