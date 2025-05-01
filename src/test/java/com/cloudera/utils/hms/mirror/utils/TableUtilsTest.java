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

package com.cloudera.utils.hms.mirror.utils;

import com.cloudera.utils.hms.mirror.domain.Cluster;
import com.cloudera.utils.hms.mirror.domain.EnvironmentTable;
import com.cloudera.utils.hms.util.TableUtils;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.cloudera.utils.hms.mirror.TablePropertyVars.TRANSACTIONAL;
import static org.junit.jupiter.api.Assertions.*;

@Slf4j
public class TableUtilsTest {

    private final List<String> table_01 = new ArrayList<>();
    private final List<String> table_02 = new ArrayList<>();
    private final List<String> table_03 = new ArrayList<>();
    private final List<String> table_04 = new ArrayList<>();
    private final List<String> table_05 = new ArrayList<>();
    private final List<String> table_06 = new ArrayList<>();
    private final List<String> table_07 = new ArrayList<>();
    private final List<String> table_08 = new ArrayList<>();


    @Test
    public void changeTableName() {
        TableUtils.changeTableName("call_center", "transfer_call_center", table_04);
        assertEquals("transfer_call_center", TableUtils.getTableNameFromDefinition(table_04));
    }

    @Test
    public void stripLocation() {
        TableUtils.stripLocation("call_center", table_04);
        assertEquals(null, TableUtils.getLocation("transfer_call_center", table_04));
    }

    @Test
    public void removeTblPropertyTransactional() {
        TableUtils.removeTblProperty(TRANSACTIONAL, table_04);
        assertEquals(null, TableUtils.getTblProperty(TRANSACTIONAL, table_04));
    }

    public List<String> fromStatic(List<String> source) {
        List<String> rtn = new ArrayList<>();
        for (String line : source) {
            rtn.add(line.trim());
        }
        return rtn;
    }

    @Test
    public void getLocation() {
        assertEquals("hdfs://HOME90/user/dstreev/datasets/call_center", TableUtils.getLocation("transfer_call_center", table_04));
    }

    @Test
    public void isACID() {
        EnvironmentTable envTable = new EnvironmentTable(null);
        envTable.setName("check_table");
        envTable.setDefinition(table_03);
        assertFalse(TableUtils.isACID(envTable));
    }

    @Test
    public void isExternal() {
        EnvironmentTable envTable = new EnvironmentTable(null);
        envTable.setName("check_table");
        envTable.setDefinition(table_02);
        assertFalse(TableUtils.isExternal(envTable));
    }

    @Test
    public void isExternalPurge() {
        EnvironmentTable envTable = new EnvironmentTable(null);
        envTable.setName("check_table");
        envTable.setDefinition(table_04);
        assertTrue(TableUtils.isExternalPurge(envTable));
    }

    @Test
    public void isHMSConverted() {
        EnvironmentTable envTable = new EnvironmentTable(null);
        envTable.setName("check_table");
        envTable.setDefinition(table_01);
        assertTrue(TableUtils.isHMSConverted(envTable));
    }

    @Test
    public void isHive3Standard() {
        EnvironmentTable envTable = new EnvironmentTable(null);
        envTable.setName("check_table");
        envTable.setDefinition(table_02);
        assertFalse(TableUtils.isHive3Standard(envTable));
    }

    @Test
    public void isLegacyManaged() {
        EnvironmentTable envTable = new EnvironmentTable(null);
        envTable.setName("check_table");
        envTable.setDefinition(table_02);
        Cluster cluster = new Cluster();
        cluster.setLegacyHive(true);
        assertTrue(TableUtils.isLegacyManaged(cluster, envTable));
    }

    @Test
    public void isManaged() {
        EnvironmentTable envTable = new EnvironmentTable(null);
        envTable.setName("check_table");
        envTable.setDefinition(table_02);

        assertTrue(TableUtils.isManaged(envTable));
        envTable.setDefinition(table_03);
        assertTrue(TableUtils.isManaged(envTable));
        envTable.setDefinition(table_04);
        assertFalse(TableUtils.isManaged(envTable));
    }

    @Test
    public void convertToIceberg_01() {
        TableUtils.convertToIceberg("test", table_01);
        log.info("Table: {}", table_01);
    }

    @Test
    public void isPartitioned() {
    }

    @Test
    public void removeTblProperty() {
    }

    @BeforeEach
    public void setUp() throws Exception {
        String[] strTable_01 = new String[]{
                "CREATE EXTERNAL TABLE `tpcds_bin_partitioned_orc_10`.`call_center`("
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
                , "  'hms-mirror_ConversionStage1'='2020-12-02 08:12:28', "
                , "  'hms-mirror_LegacyManaged'='true', "
                , "  'hms-mirror_Converted'='true', "
                , "  'last_modified_by'='dstreev', "
                , "  'last_modified_time'='1606919590', "
                , "  'transient_lastDdlTime'='1606919590')"
        };
        table_01.addAll(Arrays.asList(strTable_01));
        String[] strTable_02 = new String[]{
                "CREATE TABLE `tpcds_bin_partitioned_orc_10`.`call_center`("
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
                , "  'hms-mirror_ConversionStage1'='2020-12-02 08:12:28', "
                , "  'hms-mirror_LegacyManaged'='true', "
                , "  'hms-mirror_Converted'='true', "
                , "  'last_modified_by'='dstreev', "
                , "  'last_modified_time'='1606919590', "
                , "  'transient_lastDdlTime'='1606919590')"
        };
        table_02.addAll(Arrays.asList(strTable_02));
        String[] strTable_03 = new String[]{
                "CREATE TABLE `tpcds_bin_partitioned_orc_10`.`call_center`("
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
                , "  'hms-mirror_ConversionStage1'='2020-12-02 08:12:28', "
                , "  'hms-mirror_LegacyManaged'='true', "
                , "  'hms-mirror_Converted'='true', "
                , "  'last_modified_by'='dstreev', "
                , "  'last_modified_time'='1606919590', "
                , "  'transient_lastDdlTime'='1606919590')"
        };
        table_03.addAll(Arrays.asList(strTable_03));
        String[] strTable_04 = new String[]{
                "CREATE EXTERNAL TABLE `tpcds_bin_partitioned_orc_10`.`call_center`("
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
                , "  'hdfs://HOME90/user/dstreev/datasets/call_center'"
                , "TBLPROPERTIES ("
                , "  'bucketing_version'='2', "
                , "  'transactional'='true', "
                , "  'external.table.purge'='true', "
                , "  'hms-mirror_ConversionStage1'='2020-12-02 08:12:28', "
                , "  'hms-mirror_LegacyManaged'='true', "
                , "  'hms-mirror_Converted'='true', "
                , "  'last_modified_by'='dstreev', "
                , "  'last_modified_time'='1606919590', "
                , "  'transient_lastDdlTime'='1606919590')"
        };
        table_04.addAll(Arrays.asList(strTable_04));

        String[] strTable_05 = new String[]{
                "CREATE EXTERNAL TABLE `hello_manager`( "
                , "  `id` bigint) "
                , "ROW FORMAT SERDE  "
                , "  'org.apache.hadoop.hive.ql.io.orc.OrcSerde'  "
                , "WITH SERDEPROPERTIES (  "
                , "  'path'='hdfs://HDP50/apps/warehouse/hive/merge_files_migrate.db/hello_manager')  "
                , "STORED AS INPUTFORMAT  "
                , "  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'  "
                , "OUTPUTFORMAT  "
                , "  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat' "
                , "LOCATION "
                , "'hdfs://HDP50/apps/warehouse/hive/merge_files_migrate.db/hello_manager' "
                , "TBLPROPERTIES ( "
                , "'hms-mirror_Metadata_Stage1'='2022-05-26 00:55:38', "
                , "'hms-mirror_LegacyManaged'='true', "
                , "'hms-mirror_Converted'='true', "
                , "'external.table.purge'='true', "
                , "  'spark.sql.create.version'='2.3.0.2.6.5.0-292',  "
                , "  'spark.sql.sources.provider'='orc',  "
                , "  'spark.sql.sources.schema.numParts'='1',  "
                , "  'spark.sql.sources.schema.part.0'='{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"long,\",\"nullable\":true,\"metadata\":{}}]}',  "
                , "  'spark.sql.statistics.numRows'='9',  "
                , "'spark.sql.statistics.totalSize'='188' "
                , "); "
        };
        table_05.addAll(Arrays.asList(strTable_05));

        String[] strTable_06 = new String[]{
                "CREATE EXTERNAL TABLE `call_center`("
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
                , "  'hdfs://HOME90/user/dstreev/datasets/call_center'"
                , "TBLPROPERTIES ("
                , "  'bucketing_version'='2', "
                , "  'transactional'='true', "
                , "  'external.table.purge'='true', "
                , "  'hms-mirror_ConversionStage1'='2020-12-02 08:12:28', "
                , "  'hms-mirror_LegacyManaged'='true', "
                , "  'hms-mirror_Converted'='true', "
                , "  'last_modified_by'='dstreev', "
                , "  'last_modified_time'='1606919590', "
                , "  'transient_lastDdlTime'='1606919590')"
        };
        table_06.addAll(Arrays.asList(strTable_06));

        String[] strTable_07 = new String[]{
                "CREATE EXTERNAL TABLE call_center("
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
                , "  'hdfs://HOME90/user/dstreev/datasets/call_center'"
                , "TBLPROPERTIES ("
                , "  'bucketing_version'='2', "
                , "  'transactional'='true', "
                , "  'external.table.purge'='true', "
                , "  'hms-mirror_ConversionStage1'='2020-12-02 08:12:28', "
                , "  'hms-mirror_LegacyManaged'='true', "
                , "  'hms-mirror_Converted'='true', "
                , "  'last_modified_by'='dstreev', "
                , "  'last_modified_time'='1606919590', "
                , "  'transient_lastDdlTime'='1606919590')"
        };
        table_07.addAll(Arrays.asList(strTable_07));

    }

    @Test
    public void testPartitionSpec() {
        String simplePartName1 = "create_date=20201201";
        String simplePartName2 = "create_date=2020_12_01/region=us-west-2";
        String simplePartName3 = "create_date=2020-12-01/region=us-west-2/zone=us-west-2a";
        String partSpec1 = TableUtils.toPartitionSpec(simplePartName1);
        String partSpec2 = TableUtils.toPartitionSpec(simplePartName2);
        String partSpec3 = TableUtils.toPartitionSpec(simplePartName3);
        System.out.println(partSpec1);
        System.out.println(partSpec2);
        System.out.println(partSpec3);
    }

    @Test
    public void testTableNameDirMatch_01() {
        List<String> tblDef = fromStatic(table_01);
        assertFalse(TableUtils.doesTableNameMatchDirectoryName(tblDef));
    }

    @Test
    public void testTableNameDirMatch_02() {
        List<String> tblDef = fromStatic(table_04);
        assertTrue(TableUtils.doesTableNameMatchDirectoryName(tblDef));
    }

    @Test
    public void testTableNameDirMatch_03() {
        List<String> tblDef = fromStatic(table_06);
        assertTrue(TableUtils.doesTableNameMatchDirectoryName(tblDef));
    }

    @Test
    public void testTableNameDirMatch_04() {
        List<String> tblDef = fromStatic(table_07);
        assertTrue(TableUtils.doesTableNameMatchDirectoryName(tblDef));
    }

    @Test
    public void testUpdateTableLocation() {
        List<String> working = fromStatic(table_05);
        String REPLACEMENT_TEST_LOCATION = "hdfs://HOME90/mynew/location";
        Boolean rtn = TableUtils.updateTableLocation("hello_manager", working, REPLACEMENT_TEST_LOCATION);
        assertEquals(TableUtils.getSerdePath("hello_manager", working), REPLACEMENT_TEST_LOCATION, "Location mismatch: ");
//        System.out.println("Def: ");
    }

    @Test
    public void updateTblProperty() {
    }
}