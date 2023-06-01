package com.cloudera.utils.hadoop.hms.mirror;

public interface SessionVars {
    String TEZ_EXECUTION_DESC = "Set 'tez' as the execution engine";
    String SET_TEZ_AS_EXECUTION_ENGINE = "set hive.execution.engine=tez";

    String SORT_DYNAMIC_PARTITION = "hive.optimize.sort.dynamic.partition";
    String SORT_DYNAMIC_PARTITION_THRESHOLD = "hive.optimize.sort.dynamic.partition.threshold";
    String ANALYZE_TABLE_STATS = "";
    String ANALYZE_COLUMN_STATS = "";
    String LEGACY_DB_LOCATION_PROP = "hive.metastore.warehouse.dir";
    String EXT_DB_LOCATION_PROP = "hive.metastore.warehouse.external.dir";
    String MNGD_DB_LOCATION_PROP = "hive.metastore.warehouse.dir";
    String HIVE_COMPRESS_OUTPUT = "hive.exec.compress.output";
    String TEZ_GROUP_MAX_SIZE = "tez.grouping.max-size";
    String HIVE_MAX_DYNAMIC_PARTITIONS = "hive.exec.max.dynamic.partitions";
    String HIVE_MAX_REDUCERS = "hive.exec.reducers.max";
}
