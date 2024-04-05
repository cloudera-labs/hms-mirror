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
import com.cloudera.utils.hms.mirror.Environment;
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
                "--hms-mirror.config.output-dir=${user.home}/.hms-mirror/test-output/e2e/cdp/sql_da_ip_left_only",
                "--hms-mirror.config.downgrade-acid=true",
                "--hms-mirror.config.in-place=true",
                "--hms-mirror.config.migrate-acid-only=true",
                "--hms-mirror.conversion.test-filename=/test_data/acid_w_parts_01.yaml",
                "--hms-mirror.config-filename=/config/default.yaml.cdp"
        })
@ActiveProfiles("e2e-cdp-sql-acid")
@Slf4j
public class Test_sql_da_ip_left_only extends E2EBaseTest {
//
//        String[] args = new String[]{"-d", "SQL",
//                "-da", "-ip", "-mao",
//                "-ltd", ACID_W_PARTS_05, "-cfg", CDP,
//                "-o", outputDir
//        };
//        long rtn = 0;
//        MirrorLegacy mirror = new MirrorLegacy();
//        rtn = mirror.go(args);
//        assertEquals("Return Code Failure: " + rtn, 0, rtn);
//
//        // Read the output and verify the results.
//        DBMirror[] resultsMirrors = getResults(outputDir,ACID_W_PARTS_05);
//

    @Test
    public void issueCountTest() {
        validateTableIssueCount("assort_test_db", "acid_03", Environment.RIGHT, 1);
    }

    @Test
    public void phaseTest() {
        validatePhase("assort_test_db", "acid_03", PhaseState.SUCCESS);
    }

    @Test
    public void returnCodeTest() {
        // Get Runtime Return Code.
        long rtn = getReturnCode();
        // Verify the return code.
        assertEquals("Return Code Failure: " + rtn, 0L, rtn);
    }

}
