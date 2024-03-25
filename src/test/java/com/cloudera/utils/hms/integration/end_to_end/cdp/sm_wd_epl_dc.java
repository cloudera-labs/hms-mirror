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

package com.cloudera.utils.hms.integration.end_to_end.cdp;

import com.cloudera.utils.hms.integration.end_to_end.E2EBaseTest;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

import static com.cloudera.utils.hms.mirror.MessageCode.STORAGE_MIGRATION_NAMESPACE_LEFT_MISSING_RDL_GLM;
import static org.junit.Assert.assertEquals;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = com.cloudera.utils.hms.Mirror.class,
        args = {
                "--hms-mirror.conversion.test-filename=/test_data/ext_purge_odd_parts.yaml",
                "--hms-mirror.config.output-dir=${user.home}/.hms-mirror/test-output/e2e/cdp/sm_wd_epl_dc"
        }
)
@ActiveProfiles("e2e-cdp-sm_wd_epl_dc")
@Slf4j
public class sm_wd_epl_dc extends E2EBaseTest {
    /*
            String[] args = new String[]{"-d", "STORAGE_MIGRATION",
                "-wd", "/finance/managed-fso",
                "-ewd", "/finance/external-fso",
//                "-smn", "ofs://OHOME90",
                "-epl",
//                "-rdl",
                "-dc",
                "-ltd", EXT_PURGE_ODD_PARTS_03, "-cfg", CDP_CDP,
                "-o", outputDir
        };
        long rtn = 0;
        MirrorLegacy mirror = new MirrorLegacy();
        rtn = mirror.go(args);
        long check = STORAGE_MIGRATION_NAMESPACE_LEFT_MISSING_RDL_GLM.getLong();

        assertEquals("Return Code Failure: " + rtn + " doesn't match: " + check * -1, check * -1, rtn);


     */
    @Test
    public void returnCodeTest() {
        // Get Runtime Return Code.
        long rtn = getReturnCode();
        // Verify the return code.
        long check = STORAGE_MIGRATION_NAMESPACE_LEFT_MISSING_RDL_GLM.getLong();
        assertEquals("Return Code Failure: " + rtn, check * -1, rtn);
    }
}
