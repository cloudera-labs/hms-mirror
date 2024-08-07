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

import com.cloudera.utils.hms.mirror.cli.Mirror;
import com.cloudera.utils.hms.mirror.integration.end_to_end.E2EBaseTest;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import static org.junit.Assert.assertEquals;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = Mirror.class,
        args = {
                "--hms-mirror.config.data-strategy=SQL",
                "--hms-mirror.conversion.test-filename=/test_data/assorted_tbls_01.yaml",
                "--hms-mirror.config.filename=/config/default.yaml.cdp-cdp.bad.hcfsns",
                "--hms-mirror.config.output-dir=${user.home}/.hms-mirror/test-output/e2e/cdp_cdp/sql_bad_hcfsns_02"
        })
@Slf4j
public class Test_sql_bad_hcfsns_02 extends E2EBaseTest {
    //        String[] args = new String[]{"-d", "SQL",
//                "-sql",
//                "-ltd", ASSORTED_TBLS_04,
//                "-cfg", CDP_CDP_BNS,
//                "-o", outputDir
//        };
//
//        long rtn = 0;
//        MirrorLegacy mirror = new MirrorLegacy();
//        rtn = mirror.go(args);
//        int check = 3; // wrong ns
//        assertEquals("Return Code Failure: " + rtn + " doesn't match: " + check, rtn, check);

    @Test
    public void returnCodeTest() {
        // Get Runtime Return Code.
        long rtn = getReturnCode();
        // Verify the return code.

        // TODO: After the changes to the Namespace handling, there is no longer a need to align the HCFS and Left Locations.
        long check = 0L;
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
//

}
