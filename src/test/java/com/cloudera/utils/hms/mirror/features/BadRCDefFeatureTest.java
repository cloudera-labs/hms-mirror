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

import com.cloudera.utils.hms.mirror.feature.BadRCDefFeature;
import com.cloudera.utils.hms.mirror.feature.Feature;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertTrue;

public class BadRCDefFeatureTest extends BaseFeatureTest {

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
    private final Feature feature = new BadRCDefFeature();

    @Test
    public void test_001() {
        List<String> schema = toList(schema_01);
        Boolean check = feature.fixSchema(schema);
        assertTrue(check);
        schema.stream().forEach(System.out::println);
    }

}