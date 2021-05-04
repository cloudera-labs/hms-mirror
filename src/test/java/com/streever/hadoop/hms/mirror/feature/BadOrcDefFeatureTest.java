package com.streever.hadoop.hms.mirror.feature;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class BadOrcDefFeatureTest {

    private String[] schema_01 = new String[]{
            "        CREATE EXTERNAL TABLE `data`(",
            "            `sys01`timestamp,",
            "            `transactiontype`string,",
            "            `record`bigint,",
            "            `host`string,",
            "            `source`string,",
            "            `envt`string,",
            "            `eventdate`int,",
            "            `logd`string)",
            "    PARTITIONED BY( ",
            "  `partitiondate` date)",
            "    ROW FORMAT DELIMITED",
            "    FIELDS TERMINATED BY '\t'",
            "    LINES TERMINATED  BY '\n'",
            "    STORED AS INPUTFORMAT ",
            "  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'",
            "    OUTPUTFORMAT ",
            "  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'",
            "    LOCATION",
            "  '/some/location' ",
            "    TBLPROPERTIES(",
            "'discover.partitions'='true',",
            "        'hive.exec.compress.intermediate'='true',",
            "        'hive.exec.compress.output'='true',",
            "        'hive.mapred.mode'='nonstrict',",
            "        'mapred.output.compression.codec'='org.apache.hadoop.io.compress.SnappyCodec',",
            "        'transient_lastDdlTime'='1555609592')"
    };

    private String[] schema_02 = new String[]{
            " CREATE EXTERNAL TABLE `data`(                 ",
            "   `systime` timestamp,                ",
            "   `transactiontype` string,                        ",
            "   `record` bigint,                               ",
            "   `host` string,                                   ",
            "   `source` string,                                 ",
            "   `env` string,                            ",
            "   `eventdate` int,                                 ",
            "   `logd` string)                                ",
            " PARTITIONED BY (                                   ",
            "   `partitiondate` date)                            ",
            " ROW FORMAT SERDE                                   ",
            "   'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'  ",
            " WITH SERDEPROPERTIES (                             ",
            "   'field.delim'='\t',                              ",
            "   'line.delim'='\n',                               ",
            "   'serialization.format'='\t')                     ",
            " STORED AS INPUTFORMAT                              ",
            "   'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'  ",
            " OUTPUTFORMAT                                       ",
            "   'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat' ",
            " LOCATION                                           ",
            "   '/some/location' ",
            " TBLPROPERTIES (                                    ",
            "   'bucketing_version'='2',                         ",
            "   'discover.partitions'='true',                    ",
            "   'hive.exec.compress.intermediate'='true',        ",
            "   'hive.exec.compress.output'='true',              ",
            "   'hive.mapred.mode'='nonstrict',                  ",
            "   'mapred.output.compression.codec'='org.apache.hadoop.io.compress.SnappyCodec',  ",
            "   'transient_lastDdlTime'='1618859429')            "
    };

    @Test
    public void test_001() {
        List<String> schemaList = new ArrayList<String>();
        schemaList.addAll(Arrays.asList(schema_01));
        Feature bof = new BadOrcDefFeature();
        List<String> newSchemaList = bof.fixSchema(schemaList);
        System.out.println("Hello");
    }
}