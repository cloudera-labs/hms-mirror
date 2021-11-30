package com.streever.hadoop.hms.mirror.feature;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertTrue;

public class BadTextfileDefFeatureTest extends BaseFeatureTest {

    private Feature feature = new BadTextFileDefFeature();

    public static String[] schema_01 = new String[]{
            "CREATE EXTERNAL TABLE `thing_two`(",
            "  `fund` int, ",
            "  `ent_id` varchar(10), ",
            "  `ent_name` varchar(100), ",
            "  `pfp_id` varchar(30), ",
            "  `rfp_name` varchar(100), ",
            "  `crnt_mos_mms` int, ",
            "  `prior_mos_mms_plus` int, ",
            "  `prior_mos_mms_mnus` int, ",
            "  `othr_mms` int, ",
            "  `tot_mms` int, ",
            "  `tot_pd` decimal(18,2), ",
            "  `file_name` varchar(100))",
            "PARTITIONED BY ( ",
            "  `rec_sys_cd` varchar(100), ",
            "  `load_datetime` bigint)",
            "ROW FORMAT DELIMITED ",
            "  FIELDS TERMINATED BY '|' ",
            "  LINES TERMINATED BY '\n' ",
            "WITH SERDEPROPERTIES ( ",
            "  'escape.delim'='\\') ",
            "STORED AS INPUTFORMAT ",
            "  'org.apache.hadoop.mapred.TextInputFormat' ",
            "OUTPUTFORMAT ",
            "  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'",
            "LOCATION",
            "'hdfs://somewherens/data/doing/something'",
            "TBLPROPERTIES (",
            "'discover.partitions'='true',",
            "  'serialization.null.format'='', ",
            "  'transient_lastDdlTime'='1468009889');"
    };

    public static String[] schema_02 = new String[]{
            "CREATE EXTERNAL TABLE `cp`(",
            "  `internal_id` bigint, ",
            "  `cm_pgm_num` varchar(30), ",
            "  `cm_pgm` varchar(50), ",
            "  `cm_pgm_type` varchar(20), ",
            "  `cm_eff_dt` varchar(50), ",
            "  `cm_term_dt` varchar(50), ",
            "  `cm_stus` varchar(20), ",
            "  `cm_stus_rsn` varchar(20), ",
            "  `cm_case_mgr` varchar(30), ",
            "  `cm_next_rvw_dt` varchar(50), ",
            "  `cm_due_dt` varchar(50), ",
            "  `cm_dgns_cd` varchar(10), ",
            "  `cm_dgns_desc` varchar(100), ",
            "  `cm_acuity_lvl` varchar(10), ",
            "  `cm_case_notes` varchar(1000), ",
            "  `ext_template` varchar(20), ",
            "  `ext_comments` varchar(1000), ",
            "  `cm_creat_dt` varchar(50), ",
            "  `cm_updt_dt` varchar(50), ",
            "  `creat_by` varchar(50), ",
            "  `update_by` varchar(50), ",
            "  `cm_prmry_case_mgr` varchar(50), ",
            "  `memb_first_name` varchar(60), ",
            "  `memb_last_name` varchar(60), ",
            "  `memb_mi` varchar(25), ",
            "  `memb_dob` varchar(50), ",
            "  `mdcd_num` varchar(50), ",
            "  `mdcr_num` varchar(50), ",
            "  `memb_elig_ext_id` varchar(100), ",
            "  `memb_elig_ext_id2` varchar(100), ",
            "  `elig_efft_dt` varchar(50), ",
            "  `elig_term_dt` varchar(50), ",
            "  `contract_num` varchar(100), ",
            "  `pbp_num` varchar(100), ",
            "  `lob` varchar(100), ",
            "  `company` varchar(100), ",
            "  `file_name` varchar(100))",
            "PARTITIONED BY ( ",
            "  `rec__cd` varchar(100), ",
            "  `load_datetime` bigint)",
            "ROW FORMAT DELIMITED ",
            "  FIELDS TERMINATED BY '|' ",
            "  LINES TERMINATED BY '\n' ",
            "WITH SERDEPROPERTIES ( ",
            "  'escape.delim'='\\') ",
            "STORED AS INPUTFORMAT ",
            "  'org.apache.hadoop.mapred.TextInputFormat' ",
            "OUTPUTFORMAT ",
            "  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'",
            "LOCATION",
            "'hdfs://somewhere/data/cp'",
            "TBLPROPERTIES (",
            "'discover.partitions'='true',",
            "  'serialization.null.format'='', ",
            "  'transient_lastDdlTime'='1470863258');"
    };

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
        assertTrue(check);
        schema.stream().forEach(System.out::println);
    }

}