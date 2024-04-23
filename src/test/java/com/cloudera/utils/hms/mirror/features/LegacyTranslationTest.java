/*
 * Copyright (c) 2022-2024. Cloudera, Inc. All Rights Reserved
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

import com.cloudera.utils.hms.mirror.feature.LegacyTranslations;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertTrue;

public class LegacyTranslationTest extends BaseFeatureTest {

    public static final String[] schema_01 = new String[]{
            "CREATE EXTERNAL TABLE `ext01`(",
            "`tms_varne` string COMMENT 'from deserializer',",
            "`flag_varne` string COMMENT 'from deserializer',",
            "`num_impianto` string COMMENT 'from deserializer',",
            "`numero_faura` string COMMENT 'from deserializer',",
            "`data_emittura` string COMMENT 'from deserializer',",
            "`data_intura` string COMMENT 'from deserializer',",
            "`data_scaura` string COMMENT 'from deserializer',",
            "`imponto` string COMMENT 'from deserializer',",
            "`imponito` string COMMENT 'from deserializer',",
            "`ivato` string COMMENT 'from deserializer',",
            "`ivaito` string COMMENT 'from deserializer',",
            "`totant` string COMMENT 'from deserializer',",
            "`proe` string COMMENT 'from deserializer')",
            "ROW FORMAT SERDE",
            "'org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe'",
            "WITH SERDEPROPERTIES (",
            "'field.delim'='\\;')",
            "STORED AS INPUTFORMAT",
            "'org.apache.hadoop.mapred.TextInputFormat'",
            "OUTPUTFORMAT",
            "'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'",
            "LOCATION",
            "'hdfs://xyz/landing/somewhere/process'",
            "TBLPROPERTIES (",
            "'bucketing_version'='2',",
            "'discover.partitions'='true',",
            "'skip.footer.line.count'='1',",
            "'skip.header.line.count'='1',",
            "'transient_lastDdlTime'='1655849156');"
    };
    private final LegacyTranslations legacyTranslations = new LegacyTranslations();

    @Test
    public void test_001() {
        List<String> schema = toList(schema_01);
        Boolean check = Boolean.FALSE;
        check = legacyTranslations.fixSchema(schema);
        schema.stream().forEach(System.out::println);
        assertTrue(check);
    }

}