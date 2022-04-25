/*
 * Copyright 2021 Cloudera, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.cloudera.utils.hadoop.hms.util;

import com.cloudera.utils.hadoop.hms.mirror.MirrorConf;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TableUtilsTest {

    private final List<String> table_01 = new ArrayList<String>();
    private final List<String> table_02 = new ArrayList<String>();
    private final List<String> table_03 = new ArrayList<String>();
    private final List<String> table_04 = new ArrayList<String>();

    @Test
    public void changeTableName() {
        List<String> tblDef = TableUtils.changeTableName("call_center", "transfer_call_center", table_04);
        tblDef = TableUtils.stripLocation("call_center", tblDef);
        TableUtils.removeTblProperty(MirrorConf.TRANSACTIONAL, tblDef);
        System.out.println("Def: ");
    }

    @Test
    public void getLocation() {
    }

    @Test
    public void isACID() {
        assertTrue(TableUtils.isACID("check_table", table_03));
    }

    @Test
    public void isExternal() {
        assertFalse(TableUtils.isExternal("check_table", table_02));
    }

    @Test
    public void isExternalPurge() {
        assertTrue(TableUtils.isExternalPurge("check_table", table_04));
    }

    @Test
    public void isHMSConverted() {
        assertTrue(TableUtils.isHMSConverted("check_table", table_01));
    }

    @Test
    public void isHive3Standard() {
        assertFalse(TableUtils.isHive3Standard("check_table", table_02));
    }

    @Test
    public void isLegacyManaged() {
//        assertTrue(TableUtils.isLegacyManaged("check_table", table_02));
    }

    @Test
    public void isManaged() {
        assertTrue(TableUtils.isManaged("check_table", table_02));
        assertTrue(TableUtils.isManaged("check_table", table_03));
        assertFalse(TableUtils.isManaged("check_table", table_04));
    }

    @Test
    public void isPartitioned() {
    }

    @Test
    public void removeTblProperty() {
    }

    @Before
    public void setUp() throws Exception {
        String[] strTable_01 = new String[]{
                "CREATE EXTERNAL TABLE `tpcds_bin_partitioned_orc_10.call_center`("
                , "  `cc_call_center_sk` bigint, "
                , "  `cc_call_center_id` char(16), "
                , "  `cc_rec_start_date` date, "
                , "  `cc_rec_end_date` date, "
                , "  `cc_closed_date_sk` bigint, "
                , "  `cc_open_date_sk` bigint, "
                , "  `cc_name` varchar(50), "
                , "  `cc_class` varchar(50), "
                , "  `cc_employees` int, "
                , "  `cc_sq_ft` int, "
                , "  `cc_hours` char(20), "
                , "  `cc_manager` varchar(40), "
                , "  `cc_mkt_id` int, "
                , "  `cc_mkt_class` char(50), "
                , "  `cc_mkt_desc` varchar(100), "
                , "  `cc_market_manager` varchar(40), "
                , "  `cc_division` int, "
                , "  `cc_division_name` varchar(50), "
                , "  `cc_company` int, "
                , "  `cc_company_name` char(50), "
                , "  `cc_street_number` char(10), "
                , "  `cc_street_name` varchar(60), "
                , "  `cc_street_type` char(15), "
                , "  `cc_suite_number` char(10), "
                , "  `cc_city` varchar(60), "
                , "  `cc_county` varchar(30), "
                , "  `cc_state` char(2), "
                , "  `cc_zip` char(10), "
                , "  `cc_country` varchar(20), "
                , "  `cc_gmt_offset` decimal(5,2), "
                , "  `cc_tax_percentage` decimal(5,2))"
                , "ROW FORMAT SERDE "
                , "  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' "
                , "STORED AS INPUTFORMAT "
                , "  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' "
                , "OUTPUTFORMAT "
                , "  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'"
                , "LOCATION"
                , "  'hdfs://HOME90/user/dstreev/datasets/junk'"
                , "TBLPROPERTIES ("
                , "  'bucketing_version'='2', "
                , "  'hmsMirror_ConversionStage1'='2020-12-02 08:12:28', "
                , "  'hmsMirror_LegacyManaged'='true', "
                , "  'hmsMirror_Converted'='true', "
                , "  'last_modified_by'='dstreev', "
                , "  'last_modified_time'='1606919590', "
                , "  'transient_lastDdlTime'='1606919590')"
        };
        table_01.addAll(Arrays.asList(strTable_01));
        String[] strTable_02 = new String[]{
                "CREATE TABLE `tpcds_bin_partitioned_orc_10.call_center`("
                , "  `cc_call_center_sk` bigint, "
                , "  `cc_call_center_id` char(16), "
                , "  `cc_rec_start_date` date, "
                , "  `cc_rec_end_date` date, "
                , "  `cc_closed_date_sk` bigint, "
                , "  `cc_open_date_sk` bigint, "
                , "  `cc_name` varchar(50), "
                , "  `cc_class` varchar(50), "
                , "  `cc_employees` int, "
                , "  `cc_sq_ft` int, "
                , "  `cc_hours` char(20), "
                , "  `cc_manager` varchar(40), "
                , "  `cc_mkt_id` int, "
                , "  `cc_mkt_class` char(50), "
                , "  `cc_mkt_desc` varchar(100), "
                , "  `cc_market_manager` varchar(40), "
                , "  `cc_division` int, "
                , "  `cc_division_name` varchar(50), "
                , "  `cc_company` int, "
                , "  `cc_company_name` char(50), "
                , "  `cc_street_number` char(10), "
                , "  `cc_street_name` varchar(60), "
                , "  `cc_street_type` char(15), "
                , "  `cc_suite_number` char(10), "
                , "  `cc_city` varchar(60), "
                , "  `cc_county` varchar(30), "
                , "  `cc_state` char(2), "
                , "  `cc_zip` char(10), "
                , "  `cc_country` varchar(20), "
                , "  `cc_gmt_offset` decimal(5,2), "
                , "  `cc_tax_percentage` decimal(5,2))"
                , "ROW FORMAT SERDE "
                , "  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' "
                , "STORED AS INPUTFORMAT "
                , "  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' "
                , "OUTPUTFORMAT "
                , "  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'"
                , "LOCATION"
                , "  'hdfs://HOME90/user/dstreev/datasets/junk'"
                , "TBLPROPERTIES ("
                , "  'bucketing_version'='2', "
                , "  'hmsMirror_ConversionStage1'='2020-12-02 08:12:28', "
                , "  'hmsMirror_LegacyManaged'='true', "
                , "  'hmsMirror_Converted'='true', "
                , "  'last_modified_by'='dstreev', "
                , "  'last_modified_time'='1606919590', "
                , "  'transient_lastDdlTime'='1606919590')"
        };
        table_02.addAll(Arrays.asList(strTable_02));
        String[] strTable_03 = new String[]{
                "CREATE TABLE `tpcds_bin_partitioned_orc_10.call_center`("
                , "  `cc_call_center_sk` bigint, "
                , "  `cc_call_center_id` char(16), "
                , "  `cc_rec_start_date` date, "
                , "  `cc_rec_end_date` date, "
                , "  `cc_closed_date_sk` bigint, "
                , "  `cc_open_date_sk` bigint, "
                , "  `cc_name` varchar(50), "
                , "  `cc_class` varchar(50), "
                , "  `cc_employees` int, "
                , "  `cc_sq_ft` int, "
                , "  `cc_hours` char(20), "
                , "  `cc_manager` varchar(40), "
                , "  `cc_mkt_id` int, "
                , "  `cc_mkt_class` char(50), "
                , "  `cc_mkt_desc` varchar(100), "
                , "  `cc_market_manager` varchar(40), "
                , "  `cc_division` int, "
                , "  `cc_division_name` varchar(50), "
                , "  `cc_company` int, "
                , "  `cc_company_name` char(50), "
                , "  `cc_street_number` char(10), "
                , "  `cc_street_name` varchar(60), "
                , "  `cc_street_type` char(15), "
                , "  `cc_suite_number` char(10), "
                , "  `cc_city` varchar(60), "
                , "  `cc_county` varchar(30), "
                , "  `cc_state` char(2), "
                , "  `cc_zip` char(10), "
                , "  `cc_country` varchar(20), "
                , "  `cc_gmt_offset` decimal(5,2), "
                , "  `cc_tax_percentage` decimal(5,2))"
                , "ROW FORMAT SERDE "
                , "  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' "
                , "STORED AS INPUTFORMAT "
                , "  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' "
                , "OUTPUTFORMAT "
                , "  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'"
                , "LOCATION"
                , "  'hdfs://HOME90/user/dstreev/datasets/junk'"
                , "TBLPROPERTIES ("
                , "  'bucketing_version'='2', "
                , "  'transactional'='true', "
                , "  'hmsMirror_ConversionStage1'='2020-12-02 08:12:28', "
                , "  'hmsMirror_LegacyManaged'='true', "
                , "  'hmsMirror_Converted'='true', "
                , "  'last_modified_by'='dstreev', "
                , "  'last_modified_time'='1606919590', "
                , "  'transient_lastDdlTime'='1606919590')"
        };
        table_03.addAll(Arrays.asList(strTable_03));
        String[] strTable_04 = new String[]{
                "CREATE EXTERNAL TABLE `tpcds_bin_partitioned_orc_10.call_center`("
                , "  `cc_call_center_sk` bigint, "
                , "  `cc_call_center_id` char(16), "
                , "  `cc_rec_start_date` date, "
                , "  `cc_rec_end_date` date, "
                , "  `cc_closed_date_sk` bigint, "
                , "  `cc_open_date_sk` bigint, "
                , "  `cc_name` varchar(50), "
                , "  `cc_class` varchar(50), "
                , "  `cc_employees` int, "
                , "  `cc_sq_ft` int, "
                , "  `cc_hours` char(20), "
                , "  `cc_manager` varchar(40), "
                , "  `cc_mkt_id` int, "
                , "  `cc_mkt_class` char(50), "
                , "  `cc_mkt_desc` varchar(100), "
                , "  `cc_market_manager` varchar(40), "
                , "  `cc_division` int, "
                , "  `cc_division_name` varchar(50), "
                , "  `cc_company` int, "
                , "  `cc_company_name` char(50), "
                , "  `cc_street_number` char(10), "
                , "  `cc_street_name` varchar(60), "
                , "  `cc_street_type` char(15), "
                , "  `cc_suite_number` char(10), "
                , "  `cc_city` varchar(60), "
                , "  `cc_county` varchar(30), "
                , "  `cc_state` char(2), "
                , "  `cc_zip` char(10), "
                , "  `cc_country` varchar(20), "
                , "  `cc_gmt_offset` decimal(5,2), "
                , "  `cc_tax_percentage` decimal(5,2))"
                , "ROW FORMAT SERDE "
                , "  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' "
                , "STORED AS INPUTFORMAT "
                , "  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' "
                , "OUTPUTFORMAT "
                , "  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'"
                , "LOCATION"
                , "  'hdfs://HOME90/user/dstreev/datasets/junk'"
                , "TBLPROPERTIES ("
                , "  'bucketing_version'='2', "
                , "  'transactional'='true', "
                , "  'external.table.purge'='true', "
                , "  'hmsMirror_ConversionStage1'='2020-12-02 08:12:28', "
                , "  'hmsMirror_LegacyManaged'='true', "
                , "  'hmsMirror_Converted'='true', "
                , "  'last_modified_by'='dstreev', "
                , "  'last_modified_time'='1606919590', "
                , "  'transient_lastDdlTime'='1606919590')"
        };
        table_04.addAll(Arrays.asList(strTable_04));

    }

    @Test
    public void updateTblProperty() {
    }
}