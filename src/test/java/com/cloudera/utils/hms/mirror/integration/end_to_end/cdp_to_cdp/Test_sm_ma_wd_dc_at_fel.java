/*
 * Copyright (c) 2024. Cloudera, Inc. All Rights Reserved
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

package com.cloudera.utils.hms.mirror.integration.end_to_end.cdp_to_cdp;

import com.cloudera.utils.hms.mirror.MessageCode;
import com.cloudera.utils.hms.mirror.integration.end_to_end.E2EBaseTest;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import static org.junit.Assert.assertEquals;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = com.cloudera.utils.hms.Mirror.class,
        args = {
                "--hms-mirror.config.data-strategy=STORAGE_MIGRATION",
                "--hms-mirror.config.migrate-acid=true",
                "--hms-mirror.config.distcp=true",
                "--hms-mirror.config.warehouse-directory=/warehouseEC/managed/hive",
                "--hms-mirror.config.external-warehouse-directory=/warehouse/external",
                "--hms-mirror.config.force-external-location=true",
                "--hms-mirror.config.global-location-map=/warehouse/tablespace/managed=/warehouseEC/managed",
                "--hms-mirror.conversion.test-filename=/test_data/acid_w_parts_01.yaml",
                "--hms-mirror.config-filename=/config/default.yaml.cdp-cdp",
                "--hms-mirror.config.output-dir=${user.home}/.hms-mirror/test-output/e2e/cdp_cdp/sm_ma_wd_dc_at_fel"
        })
@Slf4j
public class Test_sm_ma_wd_dc_at_fel extends E2EBaseTest {
    //        String[] args = new String[]{
//                "-d", "STORAGE_MIGRATION",
//                "-ma",
//                "-dc",
//                "-fel",
//                "-wd", "/warehouseEC/managed/hive", "-ewd", "/warehouse/external",
//                "-glm", "/warehouse/tablespace/managed=/warehouseEC/managed",
//                "-ltd", ACID_W_PARTS_05,
//                "-cfg", CDP_CDP,
//                "-o", outputDir
//        };
//
//        long rtn = 0;
//        MirrorLegacy mirror = new MirrorLegacy();
//        rtn = mirror.go(args);
//        int check = 0;
//        assertEquals("Return Code Failure: " + rtn + " doesn't match: " + check * -1, check * -1, rtn);
    @Test
    public void returnCodeTest() {
        // Get Runtime Return Code.
        long rtn = getReturnCode();
        // Verify the return code.
        long check = getCheckCode(MessageCode.DISTCP_REQUIRES_EPL);
        assertEquals("Return Code Failure: " + rtn, check, rtn);
    }

//    @Test
//    public void phaseTest() {
//        validatePhase("ext_purge_odd_parts", "web_sales", PhaseState.SUCCESS);
//    }
//
//    @Test
//    public void issueTest() {
//        validateTableIssueCount("ext_purge_odd_parts", "web_sales",
//                Environment.LEFT, 17);
//    }

}
