package com.cloudera.utils.hadoop.hms;

public interface TestSQL {

    String CREATE_LEGACY_ACID_TBL_N_BUCKETS = "CREATE TABLE IF NOT EXISTS {0} " +
            "(id String" +
            ", checkValue String) " +
            "CLUSTERED BY (id) INTO {1} BUCKETS " +
            "STORED AS ORC " +
            "TBLPROPERTIES (\"transactional\"=\"true\")";
    String TBL_INSERT = "INSERT INTO TABLE {0} " +
            "VALUES {1}";
    String TBL_INSERT_PARTITIONED = "INSERT INTO TABLE {0} PARTITION (num) " +
            "VALUES {1}";
    String CREATE_LEGACY_ACID_TBL_N_BUCKETS_PARTITIONED = "CREATE TABLE IF NOT EXISTS {0} " +
            "(id String" +
            ", checkValue String) " +
            "PARTITIONED BY (num String) " +
            "CLUSTERED BY (id) INTO {1} BUCKETS " +
            "STORED AS ORC " +
            "TBLPROPERTIES (\"transactional\"=\"true\")";

    String CREATE_ACID_TBL_N_BUCKETS = "CREATE TABLE IF NOT EXISTS {0} " +
            "(id String, checkValue String) " +
            "CLUSTERED BY (id) INTO {1} BUCKETS " +
            "STORED AS ORC";
    String CREATE_ACID_TBL = "CREATE TABLE {0} " +
            "(id String, checkValue String)";
    String CREATE_EXTERNAL_TBL = "CREATE EXTERNAL TABLE IF NOT EXISTS {0} " +
            "(id String, checkValue String)";
    String CREATE_LEGACY_MNGD_TBL = "CREATE TABLE IF NOT EXISTS {0} " +
            "(id String, checkValue String)";
    String CREATE_EXTERNAL_TBL_PARTITIONED = "CREATE EXTERNAL TABLE IF NOT EXISTS {0} " +
            "(id String, checkValue String)" +
            "PARTITIONED BY (num String) " +
            "STORED AS ORC";

    // Escape single quotes with another quote when using MessageFormat.format IE: ' to '' .
    String CREATE_AVRO_TBL_SHORT = "CREATE TABLE {0} IF NOT EXISTS " +
            "STORED AS AVRO " +
            "TBLPROPERTIES (" +
            "''avro.schema.url''=''{1}'')";
//            "'avro.schema.url'='hdfs://HOME90/user/dstreev/avro/test.avsc')";

    String CREATE_AVRO_TBL_LONG = "CREATE TABLE IF NOT EXISTS `{0}` (" +
            "  `field1` string COMMENT ''," +
            "  `field2` int COMMENT ''," +
            "  `field3` bigint COMMENT ''," +
            "  `field4` boolean COMMENT '')" +
            "ROW FORMAT SERDE" +
            "  'org.apache.hadoop.hive.serde2.avro.AvroSerDe'" +
            "STORED AS INPUTFORMAT" +
            "  'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'" +
            "OUTPUTFORMAT" +
            "  'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'" +
            "TBLPROPERTIES (" +
            "  'bucketing_version'='2'," +
            "  'transactional'='true'," +
            "  'transactional_properties'='insert_only'," +
            "  'transient_lastDdlTime'='1656593901'," +
            "  'avro.schema.url'='{1}')";
//            "  'avro.schema.url'='hdfs://HOME90/user/dstreev/avro/test.avsc')";
}
