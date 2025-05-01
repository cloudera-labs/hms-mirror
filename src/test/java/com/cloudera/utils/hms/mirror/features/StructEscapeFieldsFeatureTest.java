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

import com.cloudera.utils.hms.mirror.feature.Feature;
import com.cloudera.utils.hms.mirror.feature.StructEscapeFieldsFeature;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class StructEscapeFieldsFeatureTest extends BaseFeatureTest {
    public static final String[] schema_01 = new String[]{
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

    public static final String[] schema_02 = new String[]{
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

    public static final String[] schema_03 = new String[]{
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

    public static final String[] schema_04 = new String[]{
            " CREATE EXTERNAL TABLE `my_error`(           ",
            "   `weird_item_id` string,                      ",
            "   `weird_id` string,                           ",
            "   `member_id` string,                              ",
            "   `group_tub_id` string,                   ",
            "   `user_assoc_id` string,                   ",
            "   `person_nbr` string,                             ",
            "   `carrier_tub_id` string,                 ",
            "   `weird_reason_cde` string,                   ",
            "   `weird_action_cde` string,                   ",
            "   `weird_disposition_cde` string,              ",
            "   `weird_resource_id` string,                  ",
            "   `start_tms` timestamp,                           ",
            "   `end_tms` timestamp,                             ",
            "   `start_timezone_txt` string,                     ",
            "   `end_timezone_txt` string,                       ",
            "   `start_tms_txt` string,                          ",
            "   `end_tms_txt` string,                            ",
            "   `attendant_agent_id` string,                     ",
            "   `weird_direction_dsc` string,                ",
            "   `originating_system_dsc` string,                 ",
            "   `weird_intent_dsc` string,                   ",
            "   `outcome_dsc` string,                            ",
            "   `electronic_mail_address_txt` string,            ",
            "   `city_nme` string,                               ",
            "   `state_or_province_cde` string,                  ",
            "   `postal_cde` string,                             ",
            "   `web_address_txt` string,                        ",
            "   `domain_nme` string,                             ",
            "   `application_uri_txt` string,                    ",
            "   `lob_dsc` string,                                ",
            "   `business_segment_dsc` string,                   ",
            "   `comment_txt` string,                            ",
            "   `street_address_lines_txt` string,               ",
            "   `business_products_dsc` string,                  ",
            "   `primary_reason_ind` string,                     ",
            "   `primary_aspect_dude_contact_file_ind` string,  ",
            "   `worksite_id` string,                            ",
            "   `order_id` string,                       ",
            "   `weird_survey_result_id` string,             ",
            "   `weird_survey_id` string,                    ",
            "   `phone_number` array<struct<channel_medium_relative_id:string,country_calling_cde:string,phone_nbr:string,phone_extension_nbr:string,phone_usage_type_dsc:string,attendant_agent_id:string,pond_source_system_dsc:string>>,  ",
            "   `weird_logging_system` array<struct<pond_source_system_dsc:string,pond_source_system_insert_tms:timestamp,pond_source_system_last_update_tms:timestamp,logging_system_dsc:string>>,  ",
            "   `correlation_special` array<struct<identifier_type_dsc:string,identifier_txt:string>>,  ",
            "   `weird_property` array<struct<property_type_dsc:string,property_txt:string>>,  ",
            "   `subject_reference_store_key` array<struct<resource_type_dsc:string,store_nme:string,key_nme:string,primary_store_ind:boolean,thingy_nme:string,thingy_value_txt:string,pond_source_system_dsc:string>>,  ",
            "   `related_reference_store_key` array<struct<resource_type_dsc:string,store_nme:string,key_nme:string,primary_store_ind:boolean,thingy_nme:string,thingy_value_txt:string,pond_source_system_dsc:string>>,  ",
            "   `weird_channel` array<struct<channel_medium_type_cde:string,channel_medium_type_dsc:string,pond_source_system_dsc:string>>,  ",
            "   `pond_last_update_tms` timestamp)           ",
            " ROW FORMAT SERDE                                   ",
            "   'org.apache.hadoop.hive.ql.io.orc.OrcSerde'      ",
            " STORED AS INPUTFORMAT                              ",
            "   'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'  ",
            " OUTPUTFORMAT                                       ",
            "   'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat' ",
            " LOCATION                                           ",
            "   'hdfs://MYCLUSTER/big_area.db/my_error' ",
            " TBLPROPERTIES (                                    ",
            "   'bucketing_version'='2',                         ",
            "   'discover.partitions'='true',                    ",
            "   'transient_lastDdlTime'='1684275534')            "
    };

    public static final String[] schema_05 = new String[]{
            " CREATE EXTERNAL TABLE `my_test`(                  ",
            "   `tell_me_period_end_dte` date,                 ",
            "   `tell_mehum_type_cde` string,                   ",
            "   `patient_agn_id` int,                            ",
            "   `adherence_condition_txt` string,                ",
            "   `tenant_id` string,                              ",
            "   `year_nbr` int,                                  ",
            "   `primary_channel_type_txt` string,               ",
            "   `measure_type_cde` string,                       ",
            "   `index_dte` date,                                ",
            "   `from_where_begin_dte` date,                  ",
            "   `from_where_end_dte` string,                  ",
            "   `from_where_cde` string,                      ",
            "   `pond_tms` timestamp,                       ",
            "   `pdc_patient_condition_report` array<struct<item_service_agn_id:string,patient_state_cde:string,overlord_operational_id:string,overlord_dude_hierarchy_level_type_cde:string,overlord_dude_id:int,group_operational_id:string,group_dude_hierarchy_level_type_cde:string,group_dude_id:int,member_id:string,primary_provider_agn_id:int,primary_provider_source_cde:int,primary_provider_practitioner_id:string,primary_prescriber_agn_id:int,primary_prescriber_source_cde:int,primary_prescriber_practitioner_id:string,drug_id:string,pharmacy_claim_agn_id:decimal(18,0),age_qty:int,gender_cde:string,covered_day_qty:int,days_supply_qty:int,treatment_day_qty:int,proportion_of_days_covered_msr:decimal(15,4),serviced_dte:date>>) ",
            " ROW FORMAT SERDE                                   ",
            "   'org.apache.hadoop.hive.ql.io.orc.OrcSerde'      ",
            " STORED AS INPUTFORMAT                              ",
            "   'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'  ",
            " OUTPUTFORMAT                                       ",
            "   'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat' ",
            " LOCATION                                           ",
            "   'hdfs://MYCLUSTER/test.db/my_test' ",
            " TBLPROPERTIES (                                    ",
            "   'bucketing_version'='2',                         ",
            "   'discover.partitions'='true',                    ",
            "   'transient_lastDdlTime'='1684275550')            "};

    private final String ESCAPE = "`";

    private final Feature feature = new StructEscapeFieldsFeature();

    @Test
    public void test_001() {
        String value = "array<struct<guid:string,when:bigint,session:string,rawstring:string,type:string>>";
        List<String> schema = toList(new String[]{value});
        String newStruct = ((StructEscapeFieldsFeature) feature).fixStruct(value);
        System.out.println(newStruct);
    }

    @Test
    public void test_002() {
        String value = "map<biging,struct<guid:string,when:bigint,session:string,rawstring:string,type:string>>";
        List<String> schema = toList(new String[]{value});
        String newStruct = ((StructEscapeFieldsFeature) feature).fixStruct(value);
        System.out.println(newStruct);
    }

    @Test
    public void test_003() {
        String value = "`restrictions` struct<reflog:boolean,refadvert:boolean,refproduct:boolean,refusegift:boolean,reftion:boolean,refemail:boolean>,";
        List<String> schema = toList(new String[]{value});
        String newStruct = ((StructEscapeFieldsFeature) feature).fixStruct(value);
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

    @Test
    public void test_013() {
        List<String> schema = toList(schema_04);
        Boolean check = feature.fixSchema(schema);
        schema.stream().forEach(System.out::println);
        assertTrue(check);
    }

    @Test
    public void test_014() {
        List<String> schema = toList(schema_05);
        Boolean check = feature.fixSchema(schema);
        schema.stream().forEach(System.out::println);
        assertTrue(check);
    }

}