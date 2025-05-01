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

package com.cloudera.utils.hms.mirror.integration.end_to_end.cdp;

import com.cloudera.utils.hms.mirror.domain.support.Environment;
import com.cloudera.utils.hms.mirror.PhaseState;
import com.cloudera.utils.hms.mirror.cli.Mirror;
import com.cloudera.utils.hms.mirror.integration.end_to_end.E2EBaseTest;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith(SpringExtension.class)
@SpringBootTest(classes = Mirror.class,
        args = {
                "--hms-mirror.config.output-dir=${user.home}/.hms-mirror/test-output/e2e/cdp/sm_smn_wd_epl_glm_fel",
                "--hms-mirror.conversion.test-filename=/test_data/ext_purge_odd_parts_01.yaml",
                "--hms-mirror.config.distcp=false",
                "--hms-mirror.config.global-location-map=/user/dstreev/datasets/alt-locations/load_web_sales=/finance/external-fso/load_web_sales,/warehouse/tablespace/external/hive=/finance/external-fso"

        })
@ActiveProfiles("e2e-cdp-sm_smn_wd_epl_dc")
@Slf4j
public class Test_sm_smn_wd_epl_glm_fel extends E2EBaseTest {
    //    @Test
//    public void sm_smn_wd_epl_glm_fel() {
//        String nameofCurrMethod = new Throwable()
//                .getStackTrace()[0]
//                .getMethodName();
//
//        String outputDir = getOutputDirBase() + nameofCurrMethod;
//
//        String[] args = new String[]{"-d", "STORAGE_MIGRATION",
//                "-wd", "/finance/managed-fso",
//                "-ewd", "/finance/external-fso",
//                "-smn", "ofs://OHOME90",
////                "-epl",
//                "-glm", "/user/dstreev/datasets/alt-locations/load_web_sales=/finance/external-fso/load_web_sales,/warehouse/tablespace/external/hive=/finance/external-fso",
//                "-fel",
////                "-dc",
//                "-ltd", EXT_PURGE_ODD_PARTS_03, "-cfg", CDP_CDP,
//                "-o", outputDir
//        };
//        long rtn = 0;
//        MirrorLegacy mirror = new MirrorLegacy();
//        rtn = mirror.go(args);
//        assertEquals("Return Code Failure: " + rtn, 0, rtn);
//
//        // Read the output and verify the results.
//        DBMirror[] resultsMirrors = getResults(outputDir,EXT_PURGE_ODD_PARTS_03);
//
//        validatePhase(resultsMirrors[0], "web_sales", PhaseState.CALCULATED_SQL);
//        validateTableIssueCount(resultsMirrors[0], "web_sales", Environment.RIGHT, 1);
//
//        if (!validateSqlPair(resultsMirrors[0], Environment.LEFT, "web_sales",  "Remove table property",
//                "ALTER TABLE web_sales UNSET TBLPROPERTIES (\"TRANSLATED_TO_EXTERNAL\")")) {
//            fail("Remove Table Property not found");
//        }
//
//    }

    @Test
    public void phaseTest() {
        validatePhase("ext_purge_odd_parts", "web_sales", PhaseState.CALCULATED_SQL);
    }

    @Test
    public void returnCodeTest() {
        // Get Runtime Return Code.
        long rtn = getReturnCode();
        // Verify the return code.
        long check = 0L;
        assertEquals(check, rtn, "Return Code Failure: " + rtn);
    }

    @Test
    public void tableIssueCountTest() {
        validateTableIssueCount("ext_purge_odd_parts", "web_sales",
                Environment.RIGHT, 19);
    }
}
