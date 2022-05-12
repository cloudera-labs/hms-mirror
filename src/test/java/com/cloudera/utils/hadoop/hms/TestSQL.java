package com.cloudera.utils.hadoop.hms;

public interface TestSQL {

    String CREATE_LEGACY_ACID_TBL_N_BUCKETS = "CREATE TABLE {0} " +
            "(id String" +
            ", checkValue String) " +
            "CLUSTERED BY (id) INTO {1} BUCKETS " +
            "STORED AS ORC " +
            "TBLPROPERTIES (\"transactional\"=\"true\")";
    String TBL_INSERT = "INSERT INTO TABLE {0} " +
            "VALUES {1}";
    String TBL_INSERT_PARTITIONED = "INSERT INTO TABLE {0} PARTITION (num) " +
            "VALUES {1}";
    String CREATE_LEGACY_ACID_TBL_N_BUCKETS_PARTITIONED = "CREATE TABLE {0} " +
            "(id String" +
            ", checkValue String) " +
            "PARTITIONED BY (num String) " +
            "CLUSTERED BY (id) INTO {1} BUCKETS " +
            "STORED AS ORC " +
            "TBLPROPERTIES (\"transactional\"=\"true\")";

    String CREATE_ACID_TBL_N_BUCKETS = "CREATE TABLE {0} " +
            "(id String, checkValue String) " +
            "CLUSTERED BY (id) INTO {1} BUCKETS " +
            "STORED AS ORC";
    String CREATE_ACID_TBL = "CREATE TABLE {0} " +
            "(id String, checkValue String)";
    String CREATE_EXTERNAL_TBL = "CREATE EXTERNAL TABLE {0} " +
            "(id String, checkValue String)";
    String CREATE_EXTERNAL_TBL_PARTITIONED = "CREATE EXTERNAL TABLE {0} " +
            "(id String, checkValue String)" +
            "PARTITIONED BY (num String) " +
            "STORED AS ORC";

}
