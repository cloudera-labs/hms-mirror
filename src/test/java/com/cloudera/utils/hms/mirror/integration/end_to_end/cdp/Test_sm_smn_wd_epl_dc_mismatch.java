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


import com.cloudera.utils.hms.mirror.integration.end_to_end.E2EBaseTest;
import com.cloudera.utils.hms.mirror.PhaseState;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

import static org.junit.Assert.assertEquals;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = com.cloudera.utils.hms.Mirror.class,
        args = {
                "--hms-mirror.conversion.test-filename=/test_data/ext_purge_odd_parts.yaml",
                "--hms-mirror.config-filename=/config/default.yaml.cdp",
                "--hms-mirror.config.output-dir=${user.home}/.hms-mirror/test-output/e2e/cdp/sm_smn_wd_epl_dc_mismatch",
        })
@ActiveProfiles("e2e-cdp-sm_smn_wd_epl_dc")
@Slf4j
/*
This test uses a configuration (LEFT hcfs namespace) that doesn't match the table/partition prefixes. This should
fail with warnings.

FIXED:1. when namespace in table doesn't match the namespace specified in the hcfsNamespace, nothing is translated.
    - This should result in an error and warnings about why this didn't work.

TODO: We need to fix the return code to be negative on Errors and Positive on 'table' conversion failures
        but success app run.
 */
public class Test_sm_smn_wd_epl_dc_mismatch extends E2EBaseTest {
    @Test
    public void phaseTest() {
        validatePhase("ext_purge_odd_parts", "web_sales", PhaseState.ERROR);
    }

    /*
            String[] args = new String[]{"-d", "STORAGE_MIGRATION",
                "-wd", "/finance/managed-fso",
                "-ewd", "/finance/external-fso",
                "-smn", "ofs://OHOME90",
                "-epl",
                "-dc",
                "-ltd", EXT_PURGE_ODD_PARTS_03, "-cfg", CDP,
                "-o", outputDir
        };
        long rtn = 0;
        MirrorLegacy mirror = new MirrorLegacy();
        rtn = mirror.go(args);
        assertEquals("Return Code Failure: " + rtn, 1, rtn);

        // Read the output and verify the results.
        DBMirror[] resultsMirrors = getResults(outputDir, EXT_PURGE_ODD_PARTS_03);

        validatePhase(resultsMirrors[0], "web_sales", PhaseState.ERROR);

     */
    @Test
    public void returnCodeTest() {
        // Get Runtime Return Code.
        long rtn = getReturnCode();
        // Verify the return code.
        long check = 1L;
        assertEquals("Return Code Failure: " + rtn, check, rtn);
    }

}
