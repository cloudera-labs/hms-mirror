/*
 * Copyright (c) 2022. Cloudera, Inc. All Rights Reserved
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

package com.cloudera.utils.hadoop.hms.mirror.feature;

import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class BadParquetDefFeatureTest extends BaseFeatureTest {

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

    public static String[] schema_03 = new String[]{
            "CREATE EXTERNAL TABLE `my_spark_tbl`(",
            "  `institution_id` int, ",
            "  `period_id` string, ",
            "  `company_id` int, ",
            "  `ebr_id` string, ",
            "  `entity_name_derived` string, ",
            "  `period_end_date` timestamp, ",
            "  `fiscal_year` smallint, ",
            "  `magnitude` string, ",
            "  `environmental_data_source` string, ",
            "  `val_1` decimal(38,19), ",
            "  `val_1_disclosure` string, ",
            "  `val_2` decimal(38,19), ",
            "  `val_2_disclosure` string, ",
            "  `val_3_upstream` decimal(38,19), ",
            "  `val_3_upstream_air_transportation_disclosure` string, ",
            "  `val_3_upstream_rail_transportation_disclosure` string, ",
            "  `val_3_upstream_truck_transportation_disclosure` string, ",
            "  `val_3_downstream` decimal(38,19), ",
            "  `val_3_downstream_disclosure` string, ",
            "  `my_scr` int, ",
            "  `my_scr_scope_1` int, ",
            "  `my_scr_scope_2` int, ",
            "  `my_scr_scope_1_scope_2` int, ",
            "  `my_scr_scope_3_upstream` int, ",
            "  `my_scr_scope_3_downstream` int, ",
            "  `my_scr_scope_3` int)",
            "PARTITIONED BY ( ",
            "  `run_frequency` string, ",
            "  `run_report_date` date, ",
            "  `run_rev` int)",
            "ROW FORMAT SERDE ",
            "  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' ",
            "WITH SERDEPROPERTIES ( ",
            "  'path'='hdfs://oldns/dev/app/XYZ/abc/df/my_spark_tbl') ",
            "STORED AS INPUTFORMAT ",
            "  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' ",
            "OUTPUTFORMAT ",
            "  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'",
            "LOCATION",
            "  'hdfs://oldns/dev/app/XYZ/abc/df/my_spark_tbl'",
            "TBLPROPERTIES (",
            "  'spark.sql.create.version'='2.3.0.2.6.5.106-2', ",
            "  'spark.sql.partitionProvider'='catalog', ",
            "  'spark.sql.sources.provider'='org.apache.spark.sql.parquet', ",
            "  'spark.sql.sources.schema.numPartCols'='3', ",
            "  'spark.sql.sources.schema.numParts'='1', ",
            "  'spark.sql.sources.schema.part.0'='{\"type\":\"struct\",\"fields\":[{\"name\":\"institution_id\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"period_id\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"company_id\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"ebr_id\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"entity_name_derived\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"period_end_date\",\"type\":\"timestamp\",\"nullable\":true,\"metadata\":{}},{\"name\":\"fiscal_year\",\"type\":\"short\",\"nullable\":true,\"metadata\":{}},{\"name\":\"magnitude\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"environmental_data_source\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"val_1\",\"type\":\"decimal(38,19)\",\"nullable\":true,\"metadata\":{}},{\"name\":\"val_1_disclosure\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"val_2\",\"type\":\"decimal(38,19)\",\"nullable\":true,\"metadata\":{}},{\"name\":\"val_2_disclosure\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"val_3_upstream\",\"type\":\"decimal(38,19)\",\"nullable\":true,\"metadata\":{}},{\"name\":\"val_3_upstream_air_transportation_disclosure\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"val_3_upstream_rail_transportation_disclosure\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"val_3_upstream_truck_transportation_disclosure\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"val_3_downstream\",\"type\":\"decimal(38,19)\",\"nullable\":true,\"metadata\":{}},{\"name\":\"val_3_downstream_disclosure\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"my_scr\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"my_scr_scope_1\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"my_scr_scope_2\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"my_scr_scope_1_scope_2\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"my_scr_scope_3_upstream\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"my_scr_scope_3_downstream\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"my_scr_scope_3\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"run_frequency\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"run_report_date\",\"type\":\"date\",\"nullable\":true,\"metadata\":{}},{\"name\":\"run_rev\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}]}', ",
            "  'spark.sql.sources.schema.partCol.0'='run_frequency', ",
            "  'spark.sql.sources.schema.partCol.1'='run_report_date', ",
            "  'spark.sql.sources.schema.partCol.2'='run_rev', ",
            "  'transient_lastDdlTime'='1658852325')"
    };

    private final Feature feature = new BadParquetDefFeature();

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
        schema.stream().forEach(System.out::println);
        assertTrue(check);
    }

    @Test
    public void test_003() {
        List<String> schema = toList(schema_03);
        Boolean check = feature.fixSchema(schema);
        schema.stream().forEach(System.out::println);
        assertFalse(check);
    }

}