/*
 * Copyright (c) 2022-2023. Cloudera, Inc. All Rights Reserved
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package com.cloudera.utils.hms.mirror.features;

import com.cloudera.utils.hms.mirror.feature.BadOrcDefFeature;
import com.cloudera.utils.hms.mirror.feature.Feature;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class BadOrcDefFeatureTest extends BaseFeatureTest {

    public static final String[] schema_01 = new String[]{
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
    public static final String[] schema_02 = new String[]{
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
    private final Feature feature = new BadOrcDefFeature();

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
        assertFalse(check);
        schema.stream().forEach(System.out::println);
    }

}