package com.streever.hadoop.hms.mirror.feature;

import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class BadParquetDefFeatureTest {

    private String[] schema_01 = new String[]{
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

    @Test
    public void test_001() {
        Feature bpf = new BadParquetDefFeature();
        List<String> newSchemaList = bpf.fixSchema(Arrays.asList(schema_01));
        newSchemaList.stream().forEach(System.out::println);
    }

//    @Test
//    public void test_002() {
//        Feature bof = new BadOrcDefFeature();
//        List<String> newSchemaList = bof.fixSchema(Arrays.asList(schema_02));
//        System.out.println(newSchemaList.toString());
//    }

}