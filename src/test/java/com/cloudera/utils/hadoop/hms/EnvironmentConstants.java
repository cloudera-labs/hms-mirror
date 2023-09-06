package com.cloudera.utils.hadoop.hms;

public interface EnvironmentConstants {
    String HDP2_CDP = "/config/default.yaml.hdp2-cdp";
    String HDP3_CDP = "/config/default.yaml.hdp3-cdp";
    String CDP_CDP = "/config/default.yaml.cdp-cdp";
    String CDP_CDP_BNS = "/config/default.yaml.cdp-cdp.bad.hcfsns";
    String CDP = "/config/default.yaml.cdp";
    String CDH_CDP = "/config/default.yaml.cdh-cdp";
    String ENCRYPTED = "/config/default.yaml.encrypted";

//    protected static final String[] execArgs = {"-e", "--accept"};

    String homedir = System.getProperty("user.home");
    String separator = System.getProperty("file.separator");

    String INTERMEDIATE_STORAGE = "s3a://my_is_bucket";
    String COMMON_STORAGE = "s3a://my_cs_bucket";

    String LEGACY_MNGD_PARTS_01 = "/test_data/legacy_mngd_parts_01.yaml";
    String LEGACY_MNGD_NO_PARTS_02 = "/test_data/legacy_mngd_no_parts.yaml";
    String EXT_PURGE_ODD_PARTS_03 = "/test_data/ext_purge_odd_parts.yaml";
    String ASSORTED_TBLS_04 = "/test_data/assorted_tbls_01.yaml";
    String ACID_W_PARTS_05 = "/test_data/acid_w_parts_01.yaml";
    String ASSORTED_TBLS_06 = "/test_data/assorted_tbls_02.yaml";
    String EXISTS_07 = "/test_data/exists_01.yaml";
    String EXISTS_PARTS_08 = "/test_data/exists_parts_02.yaml";

}
