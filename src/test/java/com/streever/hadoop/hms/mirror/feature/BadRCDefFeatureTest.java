package com.streever.hadoop.hms.mirror.feature;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class BadRCDefFeatureTest {

    private String[] schema_01 = new String[]{
            "CREATE EXTERNAL TABLE `data`(",
            "    `sys01`timestamp,",
            "    `transactiontype`string,",
            "    `record`bigint,",
            "    `host`string,",
            "    `source`string,",
            "    `envt`string,",
            "    `eventdate`int,",
            "    `logd`string)",
            " PARTITIONED BY( ",
            "  `partitiondate` date)",
            " ROW FORMAT DELIMITED",
            "    FIELDS TERMINATED BY '|'",
            " STORED AS INPUTFORMAT ",
            "  'org.apache.hadoop.hive.ql.io.RCFileInputFormat'",
            " OUTPUTFORMAT ",
            "  'org.apache.hadoop.hive.ql.io.RCFileOutputFormat'",
            " LOCATION",
            "  '/some/location' ",
            " TBLPROPERTIES(",
            "   'hive.exec.compress.intermediate'='true',",
            "   'hive.exec.compress.output'='true',",
            "   'hive.mapred.mode'='nonstrict',",
            "   'mapred.output.compression.codec'='org.apache.hadoop.io.compress.SnappyCodec',",
            "   'transient_lastDdlTime'='1555609592')"
    };

    @Test
    public void test_001() {
        List<String> schemaList = new ArrayList<String>();
        schemaList.addAll(Arrays.asList(schema_01));
        Feature bof = new BadRCDefFeature();
        List<String> newSchemaList = bof.fixSchema(schemaList);
        System.out.println("Hello");
    }
}