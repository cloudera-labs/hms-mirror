/*
 * Copyright (c) 2023-2024. Cloudera, Inc. All Rights Reserved
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

package com.cloudera.utils.hms;

import java.nio.file.FileSystems;

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
    String separator = FileSystems.getDefault().getSeparator();

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
