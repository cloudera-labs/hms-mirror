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
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

import static org.junit.Assert.assertEquals;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = Mirror.class,
        args = {
                "--hms-mirror.config.target-namespace=ofs://OHOME90",
                "--hms-mirror.config.output-dir=${user.home}/.hms-mirror/test-output/e2e/cdp/sm_wd_epl_glm_dc",
                "--hms-mirror.conversion.test-filename=/test_data/ext_purge_odd_parts_01.yaml",
                "--hms-mirror.config.global-location-map=/user/dstreev/datasets/alt-locations/load_web_sales=/finance/external-fso/load_web_sales," +
                        "/warehouse/tablespace/external/hive=/finance/external-fso,/user/dstreev/datasets/alt-locations=/finance/external-fso/ext_purge_odd_parts.db"

                // /user/dstreev/datasets/alt-locations /warehouse/tablespace/external/hive/ext_purge_odd_parts.db
        })
@ActiveProfiles("e2e-cdp-sm_wd_epl_dc")
@Slf4j
/*
Issues: Need to post warning when table/partition(s) new location isn't in the -[e]wd location.
*/
public class Test_sm_wd_epl_glm_dc extends E2EBaseTest {
    /*

        String[] args = new String[]{"-d", "STORAGE_MIGRATION",
                "-wd", "/finance/managed-fso",
                "-ewd", "/finance/external-fso",
                "-epl",
                "-glm", "/user/dstreev/datasets/alt-locations/load_web_sales=/finance/external-fso/load_web_sales,/warehouse/tablespace/external/hive=/finance/external-fso",
                "-dc",
                "-ltd", EXT_PURGE_ODD_PARTS_03, "-cfg", CDP_CDP,
                "-o", outputDir
        };
        long rtn = 0;
        MirrorLegacy mirror = new MirrorLegacy();
        rtn = mirror.go(args);
        assertEquals("Return Code Failure: " + rtn, 1, rtn);

        // Read the output and verify the results.
        DBMirror[] resultsMirrors = getResults(outputDir, EXT_PURGE_ODD_PARTS_03);

        validatePhase(resultsMirrors[0], "web_sales", PhaseState.ERROR);
        validateTableIssueCount(resultsMirrors[0], "web_sales", Environment.LEFT, 3);
        // One of the locations is not accounted for in the glm and isn't standard.  So we can't translate it..
     */

    @Test
    public void phaseTest() {
        validatePhase("ext_purge_odd_parts", "web_sales", PhaseState.ERROR);
    }

    @Test
    public void returnCodeTest() {
        // Get Runtime Return Code.
        long rtn = getReturnCode();
        // Verify the return code.
        assertEquals("Return Code Failure: " + rtn, 1L, rtn);
    }

    @Test
    public void validateTableIssueCount() {
        validateTableIssueCount("ext_purge_odd_parts", "web_sales",
                Environment.LEFT, 2);

//        assertEquals("Issue Count not as expected", 3,
//                getConversion().getDatabase("ext_purge_odd_parts")
//                        .getTableMirrors().get("web_sales")
//                        .getEnvironmentTable(Environment.LEFT).getIssues().size());
    }

}
