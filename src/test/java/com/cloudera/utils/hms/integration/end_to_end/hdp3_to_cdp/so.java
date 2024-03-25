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

package com.cloudera.utils.hms.integration.end_to_end.hdp3_to_cdp;

import com.cloudera.utils.hms.integration.end_to_end.E2EBaseTest;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import static org.junit.Assert.assertEquals;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = com.cloudera.utils.hms.Mirror.class,
        args = {
                "--hms-mirror.config.data-strategy=SCHEMA_ONLY",
                "--hms-mirror.conversion.test-filename=/test_data/legacy_mngd_parts_01.yaml",
                "--hms-mirror.config-filename=/config/default.yaml.hdp3-cdp",
                "--hms-mirror.config.output-dir=${user.home}/.hms-mirror/test-output/e2e/hdp3_cdp/sql_mao_wd_da_rdl_dc"
        })
@Slf4j
public class so extends E2EBaseTest {
    //        String[] args = new String[]{"-d", "SCHEMA_ONLY",
//                "-ltd", LEGACY_MNGD_PARTS_01,
//                "-cfg", HDP3_CDP,
//                "-o", outputDir
//        };
//        long rtn = 0;
//        MirrorLegacy mirror = new MirrorLegacy();
//        rtn = mirror.go(args);
//        assertEquals("Return Code Failure: " + rtn, 0, rtn);

    @Test
    public void returnCodeTest() {
        // Get Runtime Return Code.
        long rtn = getReturnCode();
        // Verify the return code.
        long check = 0L;
        assertEquals("Return Code Failure: " + rtn, check * -1, rtn);
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
