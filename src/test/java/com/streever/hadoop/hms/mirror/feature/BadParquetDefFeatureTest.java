package com.streever.hadoop.hms.mirror.feature;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertTrue;

public class BadParquetDefFeatureTest extends BaseFeatureTest {

    private Feature feature = new BadParquetDefFeature();

    public static String[] schema_01 = new String[]{
            "CREATE EXTERNAL TABLE `bad_parquet_legacy`(                         ",
            "  `ticker` string,                                                  ",
            "  `field1` string,                                                  ",
            "  `field2` string,                                                  ",
            "  `field3` decimal(18,6),                                           ",
            "  `field4` date,                                                    ",
            "  `field5` string,                                                  ",
            "  `field6` string,                                                  ",
            "  `field7` string,                                                  ",
            "  `field8` string,                                                  ",
            "  `field9` string,                                                  ",
            "  `field10` string,                                                 ",
            "  `field11` string,                                                 ",
            "  `field12` string,                                                 ",
            "  `created_by` string,                                              ",
            "  `created_timestamp` timestamp)                                    ",
            "PARTITIONED BY (                                                    ",
            "  `file_dt` int)                                                    ",
            "ROW FORMAT DELIMITED                                                ",
            "  FIELDS TERMINATED BY '|'                                          ",
            "  LINES TERMINATED BY '\n'                                          ",
            "STORED AS INPUTFORMAT                                               ",
            "  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'   ",
            "OUTPUTFORMAT                                                        ",
            "  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'  ",
            "TBLPROPERTIES (                                                     ",
            "  'PARQUET.COMPRESSION'='GZIP')                                     "
    };

    public static String[] schema_02 = new String[]{
            "CREATE EXTERNAL TABLE `data02`(",
            "  `field1` string,",
            "  `field2` string,",
            "  `field3` string,",
            "  `field4` string,",
            "  `field5` string,",
            "  `field6` string,",
            "  `field7` decimal(18,6),",
            "  `field8` decimal(18,6),",
            "  `field9` decimal(18,6),",
            "  `field10` int,",
            "  `field11` int,",
            "  `field12` string,",
            "  `field13` string,",
            "  `field14` string,",
            "  `field15` string,",
            "  `field16` string,",
            "  `field17` string,",
            "  `field17` timestamp)",
            "PARTITIONED BY (",
            "  `field23` string,",
            "  `field24` int,",
            "  `field25` string)",
            "ROW FORMAT DELIMITED",
            "  FIELDS TERMINATED BY '|'",
            "  LINES TERMINATED BY '\n'",
            "STORED AS INPUTFORMAT",
            "  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'",
            "OUTPUTFORMAT",
            "  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'",
            "LOCATION",
            "'hdfs://somewhere'",
            "TBLPROPERTIES (",
            "'hmsMirror_Metadata_Stage1'='2021-11-23 14:53:59',",
            "'discover.partitions'='true',",
            "  'PARQUET.COMPRESSION'='GZIP',",
            "  'transient_lastDdlTime'='1631064805')"
    };

    @Test
    public void test_001() {
        List<String> schema = toList(schema_01);
        Boolean check = feature.fixSchema(schema);
        assertTrue(check);
        schema.stream().forEach(System.out::println);
    }

    @Test
    public void test_002() {
        List<String> schema = toList(schema_02);
        Boolean check = feature.fixSchema(schema);
        assertTrue(check);
        schema.stream().forEach(System.out::println);

    }

}