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

import com.cloudera.utils.hms.mirror.cli.Mirror;
import com.cloudera.utils.hms.mirror.domain.support.Environment;
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
                "--hms-mirror.config.output-dir=${user.home}/.hms-mirror/test-output/e2e/cdp/sql_leg_mngd_bucket_ip_already",
//                "--hms-mirror.config.downgrade-acid=true",
                "--hms-mirror.config.in-place=true",
                "--hms-mirror.config.migrate-acid=10",
                "--hms-mirror.conversion.test-filename=/test_data/legacy_mngd_no_parts_ip.yaml",
                "--hms-mirror.config.filename=/config/default.yaml.cdp-cdp",
                "--hms-mirror.config.right-is-disconnected=true"
        })
@ActiveProfiles("e2e-cdp-sql-acid")
@Slf4j
public class Test_sql_leg_mngd_bucket_ip_already extends E2EBaseTest {
//
//        String[] args = new String[]{"-d", "SQL",
//                "-da", "-ip", "-mao", "-rid",
//                "-ltd", LEGACY_MNGD_NO_PARTS_02, "-cfg", CDP_CDP,
//                "-o", outputDir
//        };
//        long rtn = 0;
//        MirrorLegacy mirror = new MirrorLegacy();
//        rtn = mirror.go(args);
//        assertEquals("Return Code Failure: " + rtn, 0, rtn);
//
//        // Read the output and verify the results.
//        DBMirror[] resultsMirrors = getResults(outputDir,LEGACY_MNGD_NO_PARTS_02);
//

    @Test
    public void returnCodeTest() {
        // Get Runtime Return Code.
        long rtn = getReturnCode();
        // Verify the return code.
        assertEquals(1L, rtn, "Return Code Failure: " + rtn);
    }

    // Ensure the table is still an ACID table.
    @Test
    public void isStillAcidTest() {
        validateTableIsACID("tpcds_bin_partitioned_orc_10", "web_sales_acid", Environment.RIGHT);
    }
}
