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

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class StructEscapeFieldsFeatureTest extends BaseFeatureTest {
    public static String[] schema_01 = new String[]{
            " CREATE TABLE `test1`(  ",
            " `batch_id` string,  ",
            " `records` array<struct<guid:string,when:bigint,session:string,rawstring:string,type:string>>)  ",
            " ROW FORMAT SERDE  ",
            " 'org.apache.hadoop.hive.ql.io.orc.OrcSerde'  ",
            " STORED AS INPUTFORMAT  ",
            " 'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'  ",
            " OUTPUTFORMAT  ",
            " 'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'  ",
            " TBLPROPERTIES (  ",
            " 'transactional'='true',  ",
            " 'transactional_properties'='default'  ",
            " )  "
    };

    public static String[] schema_02 = new String[]{
            "CREATE EXTERNAL TABLE `entity`",
            "`restrictions` struct<reflog:boolean,refadvert:boolean,refproduct:boolean,refusegift:boolean,reftion:boolean,refemail:boolean>,",
            "`phone` string,",
            "`email` string,",
            "`establishmentidentificationnumber` string,",
            "`id` string,",
            "`name` string,",
            "`companyid` string,",
            "`paymentmethod` string,",
            "`paymentterm` string,",
            "`pomandatory` boolean,",
            "`salesgroup` string,",
            "`siccode` string,",
            "`vatnumber` string,",
            "`companyaddress` struct<city:string,district:string,postalcode:string,street:string,transportzone:string,country:string,building:string,floor:string,room:string,region:string,poty:string,pocode:string,deleted:boolean>,",
            "`updatedate` date,",
            "`updatedatetime` timestamp,",
            "`creationdate` date,",
            "`creationdatetime` timestamp,",
            "`custorigintype` string,",
            "`bracketemployees` string)",
            "PARTITIONED BY (",
            "`country` string,",
            "`dt_ingestion` date)",
            "ROW FORMAT SERDE",
            "'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'",
            "STORED AS INPUTFORMAT",
            "'org.apache.hadoop.mapred.TextInputFormat'",
            "OUTPUTFORMAT",
            "'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'",
            "LOCATION",
            "'hdfs://xxxxxx'",
            "TBLPROPERTIES (",
            "'discover.partitions'='true',",
            "'bucketing_version'='2')"
    };

    public static String[] schema_03 = new String[]{
            " CREATE TABLE `test1`(  ",
            " `batch_id` string,  ",
            " `records` map<biging,struct<guid:string,when:bigint,session:string,rawstring:string,type:string>>,  ",
            " `records2` array<STRUCT<guid:string,when:bigint,session:string,rawstring:string,type:string>>)  ",
            " ROW FORMAT SERDE  ",
            " 'org.apache.hadoop.hive.ql.io.orc.OrcSerde'  ",
            " STORED AS INPUTFORMAT  ",
            " 'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'  ",
            " OUTPUTFORMAT  ",
            " 'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'  ",
            " TBLPROPERTIES (  ",
            " 'transactional'='true',  ",
            " 'transactional_properties'='default'  ",
            " )  "
    };


    private final String ESCAPE = "`";

    private final Feature feature = new StructEscapeFieldsFeature();

    @Test
    public void test_001() {
        String value = "array<struct<guid:string,when:bigint,session:string,rawstring:string,type:string>>";
        List<String> schema = toList(new String[]{value});
        String newStruct = ((StructEscapeFieldsFeature)feature).fixStruct(value);
        System.out.println(newStruct);
    }

    @Test
    public void test_002() {
        String value = "map<biging,struct<guid:string,when:bigint,session:string,rawstring:string,type:string>>";
        List<String> schema = toList(new String[]{value});
        String newStruct = ((StructEscapeFieldsFeature)feature).fixStruct(value);
        System.out.println(newStruct);
    }

    @Test
    public void test_003() {
        String value = "`restrictions` struct<reflog:boolean,refadvert:boolean,refproduct:boolean,refusegift:boolean,reftion:boolean,refemail:boolean>,";
        List<String> schema = toList(new String[]{value});
        String newStruct = ((StructEscapeFieldsFeature)feature).fixStruct(value);
        System.out.println(newStruct);
    }
    @Test
    public void test_010() {
        List<String> schema = toList(schema_01);
        Boolean check = feature.fixSchema(schema);
        schema.stream().forEach(System.out::println);
        assertTrue(check);
    }

    @Test
    public void test_011() {
        List<String> schema = toList(schema_02);
        Boolean check = feature.fixSchema(schema);
        schema.stream().forEach(System.out::println);
        assertTrue(check);
    }

    @Test
    public void test_012() {
        List<String> schema = toList(schema_03);
        Boolean check = feature.fixSchema(schema);
        schema.stream().forEach(System.out::println);
        assertTrue(check);
    }

}