package com.cloudera.utils.hadoop.hms.mirror.feature;

import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertTrue;

public class BadRCDefFeatureTest extends BaseFeatureTest {

    private Feature feature = new BadRCDefFeature();

    public static String[] schema_01 = new String[]{
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
        List<String> schema = toList(schema_01);
        Boolean check = feature.fixSchema(schema);
        assertTrue(check);
        schema.stream().forEach(System.out::println);
    }

}